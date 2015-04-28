package org.atlasapi.remotesite.knowledgemotion.topics.spotlight;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;

import org.atlasapi.remotesite.knowledgemotion.topics.KeywordsExtractor;
import org.atlasapi.remotesite.knowledgemotion.topics.WikipediaKeyword;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import com.google.common.io.CharStreams;
import com.metabroadcast.common.http.HttpResponsePrologue;
import com.metabroadcast.common.http.HttpResponseTransformer;
import com.metabroadcast.common.http.SimpleHttpClient;
import com.metabroadcast.common.http.SimpleHttpClientBuilder;
import com.metabroadcast.common.http.SimpleHttpRequest;
import com.metabroadcast.common.media.MimeType;
import com.metabroadcast.common.url.UrlEncoding;

public class SpotlightKeywordsExtractor implements KeywordsExtractor {

    private static final Logger log = LoggerFactory.getLogger(SpotlightKeywordsExtractor.class);

    private static final String DBPEDIA_CONFIDENCE = "0.01";
    private static final String DBPEDIA_SUPPORT = "2";
    private static final String DBPEDIA_SPOTLIGHT_URL_PATTERN = "http://spotlight.sztaki.hu:2222/rest/annotate?text=%s&confidence=%s&support=%s";

    private final SpotlightResourceParser parser;
    private final SimpleHttpClient httpClient;

    public SpotlightKeywordsExtractor(SpotlightResourceParser parser) {
        this.parser = checkNotNull(parser);
        this.httpClient = new SimpleHttpClientBuilder()
            .withAcceptHeader(MimeType.APPLICATION_JSON)
            .build();
    }

    @Override
    public List<WikipediaKeyword> extractKeywordsFromText(String text) {
        String encodedText = UrlEncoding.encode(text);
        String url = String.format(DBPEDIA_SPOTLIGHT_URL_PATTERN, encodedText, DBPEDIA_CONFIDENCE, DBPEDIA_SUPPORT);
        String response;
        try {
            response = getResponse(url);
            String decodedText = UrlEncoding.decode(response);
            return parser.parse(decodedText);
        } catch (Exception e) {
            throw new RuntimeException("Spotlight call failed.", e);
        }
    }

    @Override
    public Optional<WikipediaKeyword> tryGuessSingleKeywordFromText(String text) {
        List<WikipediaKeyword> keywords = extractKeywordsFromText(text);
        try {
            return Optional.of(Iterables.getOnlyElement(keywords));
        } catch (Exception e) {
            return Optional.absent();  // If there is not only one match, we must quietly fail :(
        }
    }

    private String getResponse(String url) throws Exception {
        SimpleHttpRequest<String> dbpediaRequest = new SimpleHttpRequest<String>(url, new HttpResponseTransformer<String>() {

            @Override
            public String transform(HttpResponsePrologue prologue, InputStream body) throws Exception {
                InputStreamReader reader = new InputStreamReader(body);
                String content = CharStreams.toString(reader);
                return content;
            }
        });
        return httpClient.get(dbpediaRequest);
    }

}

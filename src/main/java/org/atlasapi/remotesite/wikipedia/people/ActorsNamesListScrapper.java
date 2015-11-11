package org.atlasapi.remotesite.wikipedia.people;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.atlasapi.remotesite.wikipedia.people.wikimodel.WikipediaCategoryMembersModel;
import org.joda.time.LocalDate;
import org.joda.time.LocalDateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Date;
import java.util.List;

public class ActorsNamesListScrapper {
    private static final Logger log = LoggerFactory.getLogger(ActorsNamesListScrapper.class);
    private static final String API_CALL = "/w/api.php?format=json&action=query&list=categorymembers&cmlimit=500&cmtitle=";
    private static final String HOST = "en.wikipedia.org";
    private static final String CONTINUE = "&cmcontinue=";
    private static final String ENCODING = "UTF-8";

    public static List<String> extractNames(String category) throws IOException {
        log.info("Started extracting actors names at {}", LocalDateTime.now());
        ObjectMapper mapper = new ObjectMapper();

        HttpClient client = new DefaultHttpClient();
        HttpGet request = new HttpGet(API_CALL + URLEncoder.encode(category, ENCODING));
        HttpHost host = new HttpHost(HOST);
        HttpResponse response = client.execute(new HttpHost(host), request);

        String json = EntityUtils.toString(response.getEntity());
        WikipediaCategoryMembersModel wiki = mapper.readValue(json, WikipediaCategoryMembersModel.class);

        ImmutableList.Builder<String> allTitles = ImmutableList.builder();
        allTitles.addAll(wiki.getAllTitles());
        while (wiki.getContinueWiki() != null) {
            String call = makeApiCall(category, wiki.getContinueWiki().getCmcontinue());
            request = new HttpGet(call);
            response = client.execute(host, request);
            json = EntityUtils.toString(response.getEntity());
            wiki = mapper.readValue(json, WikipediaCategoryMembersModel.class);
            allTitles.addAll(wiki.getAllTitles());
        }

        log.info("Finished extracting actors names at {}", LocalDateTime.now());
        return allTitles.build();
    }


    private static String makeApiCall(String category, String pageContinue) throws UnsupportedEncodingException {
        String title = API_CALL + category + CONTINUE + URLEncoder.encode(pageContinue, ENCODING);
        return title;
    }
}

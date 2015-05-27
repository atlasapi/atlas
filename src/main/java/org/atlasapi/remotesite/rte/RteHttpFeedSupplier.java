package org.atlasapi.remotesite.rte;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.TimeUnit;

import nu.xom.Builder;
import nu.xom.Document;

import org.atlasapi.remotesite.HttpClients;

import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.google.common.base.Supplier;
import com.metabroadcast.common.http.HttpException;
import com.metabroadcast.common.http.HttpResponsePrologue;
import com.metabroadcast.common.http.HttpResponseTransformer;
import com.metabroadcast.common.http.SimpleHttpClient;
import com.metabroadcast.common.http.SimpleHttpClientBuilder;
import com.metabroadcast.common.http.SimpleHttpRequest;


public class RteHttpFeedSupplier implements Supplier<Document> {

    private final String feedUrl;
    private final SimpleHttpClient httpClient;
    private final Builder parser = new Builder();
    
    public RteHttpFeedSupplier(String feedUrl) {
        this.httpClient = buildFetcher();
        this.feedUrl = checkNotNull(feedUrl);
    }

    @Override
    public Document get() {
        try {
            return httpClient.get(new SimpleHttpRequest<Document>(feedUrl, TRANSFORMER));
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    private SimpleHttpClient buildFetcher() {
        return new SimpleHttpClientBuilder()
            .withUserAgent(HttpClients.ATLAS_USER_AGENT)
            .withSocketTimeout(10, TimeUnit.SECONDS)
            .withRetries(3)
            .build();
    }
    
    private final HttpResponseTransformer<Document> TRANSFORMER = new HttpResponseTransformer<Document>() {
        @Override
        public Document transform(HttpResponsePrologue response, InputStream in) throws HttpException, IOException {
            try {
                return parser.build(in);
            } catch (Exception e) {
                Throwables.propagate(e);
            }
            return null;
        }
    };
}

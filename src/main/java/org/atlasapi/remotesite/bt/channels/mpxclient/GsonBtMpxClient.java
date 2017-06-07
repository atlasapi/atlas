package org.atlasapi.remotesite.bt.channels.mpxclient;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.InputStream;
import java.io.InputStreamReader;

import com.google.common.base.Optional;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.metabroadcast.common.http.HttpException;
import com.metabroadcast.common.http.HttpResponsePrologue;
import com.metabroadcast.common.http.HttpResponseTransformer;
import com.metabroadcast.common.http.HttpStatusCode;
import com.metabroadcast.common.http.SimpleHttpClient;
import com.metabroadcast.common.http.SimpleHttpRequest;
import com.metabroadcast.common.query.Selection;


public class GsonBtMpxClient implements BtMpxClient {

    private final SimpleHttpClient httpClient;
    private final String baseUri;
    private final Gson gson = new GsonBuilder()
                                    .create();
    
    public GsonBtMpxClient(SimpleHttpClient httpClient, String baseUri) {
        this.httpClient = checkNotNull(httpClient);
        this.baseUri = checkNotNull(baseUri);
    }
    
    @Override
    public PaginatedEntries getChannels(Optional<Selection> selection) throws BtMpxClientException {
        if (selection.isPresent()) {
            throw new UnsupportedOperationException("Pagination not yet supported");
        }
        
        final String uri = channelUriFor(selection);
        try {
            return httpClient.get(httpRequestFor(uri));
        } catch (Exception e) {
            throw new BtMpxClientException(e);
        } 
    }
    
    @Override
    public PaginatedEntries getCategories(Optional<Selection> selection) throws BtMpxClientException {
        if (selection.isPresent()) {
            throw new UnsupportedOperationException("Pagination not yet supported");
        }
        
        final String uri = categoryUriFor(selection);
        try {
            return httpClient.get(httpRequestFor(uri));
        } catch (Exception e) {
            throw new BtMpxClientException(e);
        } 
    }

    private SimpleHttpRequest<PaginatedEntries> httpRequestFor(final String uri) {
        return SimpleHttpRequest.httpRequestFrom(uri, 
                new HttpResponseTransformer<PaginatedEntries>() {

                    @Override
                    public PaginatedEntries transform(HttpResponsePrologue prologue, InputStream body)
                            throws HttpException, Exception {
                        if(HttpStatusCode.OK.code() == prologue.statusCode()) {
                            return gson.fromJson(new InputStreamReader(body), PaginatedEntries.class);
                        }
                        throw new HttpException(String.format("Request %s failed: %s %s", 
                                uri, prologue.statusCode(), prologue.statusLine()), prologue);
                    }
                });
    }

    private String channelUriFor(Optional<Selection> selection) {
        return baseUri + "?form=cjson";
    }
    
    private String categoryUriFor(Optional<Selection> selection) {
        return baseUri + "/categories?form=cjson";
    }
}

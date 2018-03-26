package org.atlasapi.http;

import com.metabroadcast.common.http.HttpResponseTransformer;
import com.metabroadcast.common.http.SimpleHttpClient;
import org.atlasapi.persistence.system.RemoteSiteClient;

import static com.metabroadcast.common.http.SimpleHttpRequest.httpRequestFrom;

public class HttpBackedRemoteSiteClient<T> implements RemoteSiteClient<T> {

    public static final <T> RemoteSiteClient<T> httpRemoteSiteClient(SimpleHttpClient client, HttpResponseTransformer<? extends T> transformer) {
        return new HttpBackedRemoteSiteClient<T>(client, transformer);
    }
    
    private final SimpleHttpClient client;
    private final HttpResponseTransformer<? extends T> transformer;

    public HttpBackedRemoteSiteClient(SimpleHttpClient client, HttpResponseTransformer<? extends T> transformer) {
        this.client = client;
        this.transformer = transformer;
    }
    
    @Override
    public T get(String uri) throws Exception {
        return client.get(httpRequestFrom(uri, transformer));
    }

}

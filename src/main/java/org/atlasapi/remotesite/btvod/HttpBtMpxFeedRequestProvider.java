package org.atlasapi.remotesite.btvod;

import com.metabroadcast.common.http.SimpleHttpRequest;
import org.atlasapi.remotesite.btvod.model.BtVodResponse;

import static com.google.common.base.Preconditions.checkNotNull;

public class HttpBtMpxFeedRequestProvider {

    private final String urlBase;

    public HttpBtMpxFeedRequestProvider(String urlBase) {
        this.urlBase = checkNotNull(urlBase);
    }


    public SimpleHttpRequest<BtVodResponse> buildRequest(String endpoint, Integer startIndex) {
        return new SimpleHttpRequest<>(
                String.format("%s%s?startIndex=%s", urlBase, endpoint, startIndex),
                new BtVodResponseTransformer()
        );
    }
}

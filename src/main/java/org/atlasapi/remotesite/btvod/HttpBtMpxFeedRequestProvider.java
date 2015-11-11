package org.atlasapi.remotesite.btvod;

import static com.google.common.base.Preconditions.checkNotNull;

import org.atlasapi.remotesite.btvod.model.BtVodResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.metabroadcast.common.http.SimpleHttpRequest;

public class HttpBtMpxFeedRequestProvider {

    private static final Logger log = LoggerFactory.getLogger(HttpBtMpxFeedRequestProvider.class);
    private final String urlBase;
    private final Optional<String> query;

    public HttpBtMpxFeedRequestProvider(String urlBase, String query) {
        this.urlBase = checkNotNull(urlBase);
        this.query = Optional.fromNullable(Strings.emptyToNull(query));
    }


    public SimpleHttpRequest<BtVodResponse> buildRequestForFeed(String endpoint, int rangeStart, int rangeEnd) {
        String url = String.format(
                "%s%s?range=%d-%d%s",
                urlBase,
                endpoint,
                rangeStart,
                rangeEnd,
                additionalQueryParams()
        );
        log.debug("Calling BT VoD MPX feed url {}", url);
        return new SimpleHttpRequest<>(
                url,
                new BtVodResponseTransformer()
        );
    }

    public SimpleHttpRequest<BtVodResponse> buildRequestForSingleAsset(String guid) {
        String url = String.format(
                "%s%s?byGuid=%s",
                urlBase,
                "btv-prd-search",
                guid
        );
        log.debug("Calling {}", url);
        return new SimpleHttpRequest<>(url, new BtVodResponseTransformer());
    }

    private String additionalQueryParams() {
        if (!query.isPresent()) {
            return "";
        }
        return String.format(
                "&q=%s",
                query.get()
        );
    }
}

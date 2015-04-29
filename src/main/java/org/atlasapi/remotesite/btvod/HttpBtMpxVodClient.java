package org.atlasapi.remotesite.btvod;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;
import com.metabroadcast.common.http.SimpleHttpClient;
import org.atlasapi.remotesite.btvod.model.BtVodEntry;
import org.atlasapi.remotesite.btvod.model.BtVodResponse;

import java.util.Iterator;

import static com.google.common.base.Preconditions.checkNotNull;

public class HttpBtMpxVodClient implements BtMpxVodClient {

    private final SimpleHttpClient httpClient;
    private final HttpBtMpxFeedRequestProvider requestProvider;

    public HttpBtMpxVodClient(SimpleHttpClient httpClient, HttpBtMpxFeedRequestProvider requestProvider) {
        this.httpClient = checkNotNull(httpClient);
        this.requestProvider = checkNotNull(requestProvider);
    }

    @Override
    public Iterator<BtVodEntry> getBtMpxFeed() throws Exception {

        return new AbstractIterator<BtVodEntry>() {
            private Integer currentIndex = 1;
            private Boolean moreData = true;
            private Iterator<BtVodEntry> currentItems = Iterators.emptyIterator();
            @Override
            protected BtVodEntry computeNext() {
                if (!currentItems.hasNext() && !moreData) {
                    return endOfData();
                }
                if (!currentItems.hasNext() && moreData) {
                    try {
                        BtVodResponse response = httpClient.get(requestProvider.buildRequest(currentIndex));
                        currentIndex = currentIndex + response.getEntryCount();
                        moreData = response.getEntryCount().equals(response.getItemsPerPage());
                        currentItems = response.getEntries().iterator();
                        if (!currentItems.hasNext()) {
                            return endOfData();
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
                return currentItems.next();
            }
        };
    }

}

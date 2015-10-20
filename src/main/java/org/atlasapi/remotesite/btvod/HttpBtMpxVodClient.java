package org.atlasapi.remotesite.btvod;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;
import com.metabroadcast.common.http.SimpleHttpClient;

import org.atlasapi.remotesite.btvod.model.BtVodEntry;
import org.atlasapi.remotesite.btvod.model.BtVodResponse;

import java.io.IOException;
import java.util.Iterator;

import static com.google.common.base.Preconditions.checkNotNull;

public class HttpBtMpxVodClient implements BtMpxVodClient {

    private static final int DEFAULT_PAGE_SIZE = 50;
    
    private final SimpleHttpClient httpClient;
    private final HttpBtMpxFeedRequestProvider requestProvider;
    private final int pageSize;

    public HttpBtMpxVodClient(SimpleHttpClient httpClient, HttpBtMpxFeedRequestProvider requestProvider) {
        this(httpClient, requestProvider, DEFAULT_PAGE_SIZE);
        
    }
    
    @VisibleForTesting
    public HttpBtMpxVodClient(SimpleHttpClient httpClient, HttpBtMpxFeedRequestProvider requestProvider, int pageSize) {
        this.httpClient = checkNotNull(httpClient);
        this.requestProvider = checkNotNull(requestProvider);
        this.pageSize = pageSize;
    }

    @Override
    public Iterator<BtVodEntry> getFeed(final String name) throws IOException {

        return new AbstractIterator<BtVodEntry>() {
            private Integer rangeStart = 1;
            private int rangeEnd = pageSize;
            private Boolean moreData = true;
            private Iterator<BtVodEntry> currentItems = Iterators.emptyIterator();

            @Override
            protected BtVodEntry computeNext() {
                if (!currentItems.hasNext() && !moreData) {
                    return endOfData();
                }
                if (!currentItems.hasNext() && moreData) {
                    BtVodResponse response;
                    try {
                        response = httpClient.get(requestProvider.buildRequest(name, rangeStart, rangeEnd));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    rangeStart = rangeEnd + 1;
                    rangeEnd = rangeStart + pageSize - 1;
                    moreData = response.getEntryCount().equals(response.getItemsPerPage());
                    currentItems = response.getEntries().iterator();
                    if (!currentItems.hasNext()) {
                        return endOfData();
                    }
                }
                return currentItems.next();
            }
        };
    }

}

package org.atlasapi.remotesite.btvod;

import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.metabroadcast.common.http.SimpleHttpClient;

import org.atlasapi.remotesite.btvod.model.BtVodEntry;
import org.atlasapi.remotesite.btvod.model.BtVodResponse;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;

public class HttpBtMpxVodClient implements BtMpxVodClient {

    private static final int DEFAULT_PAGE_SIZE = 50;
    
    private final SimpleHttpClient httpClient;
    private final HttpBtMpxFeedRequestProvider requestProvider;
    private final int pageSize;
    private final LoadingCache<String, Optional<BtVodEntry>> guidLookupCache = CacheBuilder.newBuilder()
            .expireAfterAccess(10, TimeUnit.SECONDS)
            .maximumSize(2000)
            .build(new CacheLoader<String, Optional<BtVodEntry>>() {
                @Override
                public Optional<BtVodEntry> load(String guid) throws Exception {
                    BtVodResponse response = httpClient.get(requestProvider.buildRequestForSingleAsset(guid));
                    return Optional.fromNullable(Iterables.getFirst(response.getEntries(), null));
                }
            });

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
                        response = httpClient.get(requestProvider.buildRequestForFeed(name, rangeStart, rangeEnd));
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

    @Override
    public Optional<BtVodEntry> getItem(String guid) {
        try {
            return guidLookupCache.get(guid);
        } catch (ExecutionException e) {
            throw Throwables.propagate(e);
        }
    }
}

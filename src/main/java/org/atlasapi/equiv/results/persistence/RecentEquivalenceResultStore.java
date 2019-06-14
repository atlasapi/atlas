package org.atlasapi.equiv.results.persistence;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import org.atlasapi.equiv.results.EquivalenceResults;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Item;

import java.util.List;

public class RecentEquivalenceResultStore implements EquivalenceResultStore {

    private final EquivalenceResultStore delegate;
    private final Cache<String, StoredEquivalenceResults> mrwItemCache;
    private final Cache<String, StoredEquivalenceResults> mrwContainerCache;

    public RecentEquivalenceResultStore(EquivalenceResultStore delegate) {
        this.delegate = delegate;
        this.mrwItemCache = CacheBuilder.newBuilder().maximumSize(50).build();
        this.mrwContainerCache = CacheBuilder.newBuilder().maximumSize(50).build();
    }
    
    @Override
    public <T extends Content> StoredEquivalenceResults store(EquivalenceResults<T> results) {
        StoredEquivalenceResults restoredResult = delegate.store(results);
        if(results.subject() instanceof Item) {
            mrwItemCache.put(results.subject().getCanonicalUri(), restoredResult);
        }
        if(results.subject() instanceof Container) {
            mrwContainerCache.put(results.subject().getCanonicalUri(), restoredResult);
        }
        return restoredResult;
    }

    @Override
    public StoredEquivalenceResults forId(String canonicalUri) {
        StoredEquivalenceResults equivalenceResults = mrwItemCache.getIfPresent(canonicalUri);
        if(equivalenceResults != null) {
            return equivalenceResults;
        }
        
        equivalenceResults = mrwContainerCache.getIfPresent(canonicalUri);
        if(equivalenceResults != null) {
            return equivalenceResults;
        }
        return delegate.forId(canonicalUri);
    }
    
    @Override
    public List<StoredEquivalenceResults> forIds(Iterable<String> canonicalUris) {
        return delegate.forIds(canonicalUris);
    }
    
    public List<StoredEquivalenceResults> latestItemResults() {
        return ImmutableList.copyOf(mrwItemCache.asMap().values());
    }

    public List<StoredEquivalenceResults> latestContainerResults() {
        return ImmutableList.copyOf(mrwContainerCache.asMap().values());
    }

}

package org.atlasapi.remotesite.bbc.nitro;

import com.google.api.client.repackaged.com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import org.atlasapi.media.entity.Identified;


public class ResolveOrFetchResult<I extends Identified> {
    
    private static final ResolveOrFetchResult<Identified> EMPTY_INSTANCE
        = new ResolveOrFetchResult<Identified>(ImmutableList.<Identified>of(), ImmutableList.<Identified>of());
    
    @SuppressWarnings("unchecked")
    public static final <I extends Identified> ResolveOrFetchResult<I> empty() {
        return (ResolveOrFetchResult<I>) EMPTY_INSTANCE;
    }

    private final ImmutableMap<String, I> resolved;
    private final ImmutableMap<String, I> fetched;
    private final ImmutableSet<I> all;

    public ResolveOrFetchResult(Iterable<I> resolved, Iterable<I> fetched) {
        this.resolved = Maps.uniqueIndex(resolved, Identified.TO_URI);
        this.fetched = Maps.uniqueIndex(fetched, Identified.TO_URI);
        this.all = ImmutableSet.copyOf(Iterables.concat(resolved, fetched));
    }

    
    public ImmutableMap<String, I> getResolved() {
        return resolved;
    }

    
    public ImmutableMap<String, I> getFetched() {
        return fetched;
    }
    
    @Override
    public String toString() {
        return Objects.toStringHelper(getClass())
                .add("resolved", resolved)
                .add("fetched", fetched)
                .toString();
    }


    public I get(String itemUri) {
        I item = resolved.get(itemUri);
        if (item == null) {
            item = fetched.get(itemUri);
        }
        return item;
    }


    public ImmutableSet<I> getAll() {
        return all;
    }
}

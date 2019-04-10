package org.atlasapi.equiv.handlers;

import org.atlasapi.equiv.results.EquivalenceResults;
import org.atlasapi.media.entity.Content;

public class DelegatingEquivalenceResultHandler<T extends Content>
        implements EquivalenceResultHandler<T> {

    private final Iterable<EquivalenceResultHandler<T>> delegates;

    public DelegatingEquivalenceResultHandler(Iterable<EquivalenceResultHandler<T>> delegates) {
        this.delegates = delegates;
    }
    
    @Override
    public boolean handle(EquivalenceResults<T> results) {
        boolean handledWithStateChange = false;
        
        for (EquivalenceResultHandler<T> delegate  : delegates) {
            handledWithStateChange |= delegate.handle(results);
        }

        return handledWithStateChange;
    }
}

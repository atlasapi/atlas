package org.atlasapi.equiv.handlers;

import org.atlasapi.equiv.results.EquivalenceResult;
import org.atlasapi.media.entity.Content;

public class BroadcastingEquivalenceResultHandler<T extends Content> implements EquivalenceResultHandler<T> {

    private final Iterable<EquivalenceResultHandler<T>> delegates;

    public BroadcastingEquivalenceResultHandler(Iterable<EquivalenceResultHandler<T>> delegates) {
        this.delegates = delegates;
    }
    
    @Override
    public void handle(
            EquivalenceResult<T> result
    ) {

        for ( EquivalenceResultHandler<T> delegate  : delegates) {
            delegate.handle(result);
        }
        
    }

}

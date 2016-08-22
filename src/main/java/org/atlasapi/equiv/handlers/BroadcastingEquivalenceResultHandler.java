package org.atlasapi.equiv.handlers;

import java.util.Optional;

import org.atlasapi.equiv.results.EquivalenceResult;
import org.atlasapi.media.entity.Content;

import com.metabroadcast.columbus.telescope.client.IngestTelescopeClientImpl;

public class BroadcastingEquivalenceResultHandler<T extends Content> implements EquivalenceResultHandler<T> {

    private final Iterable<EquivalenceResultHandler<T>> delegates;

    public BroadcastingEquivalenceResultHandler(Iterable<EquivalenceResultHandler<T>> delegates) {
        this.delegates = delegates;
    }
    
    @Override
    public void handle(
            EquivalenceResult<T> result,
            Optional<String> taskId
    ) {

        for ( EquivalenceResultHandler<T> delegate  : delegates) {
            delegate.handle(result, taskId);
        }
        
    }

}

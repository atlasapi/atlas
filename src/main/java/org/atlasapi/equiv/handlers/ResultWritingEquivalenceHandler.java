package org.atlasapi.equiv.handlers;

import java.util.Optional;

import org.atlasapi.equiv.results.EquivalenceResult;
import org.atlasapi.equiv.results.persistence.EquivalenceResultStore;
import org.atlasapi.media.entity.Content;

import com.metabroadcast.columbus.telescope.client.IngestTelescopeClientImpl;

public class ResultWritingEquivalenceHandler<T extends Content> implements EquivalenceResultHandler<T> {

    private EquivalenceResultStore store;

    public ResultWritingEquivalenceHandler(EquivalenceResultStore store) {
        this.store = store;
    }
    
    @Override
    public void handle(
            EquivalenceResult<T> result,
            Optional<String> taskId
    ) {
        store.store(result);
    }

}

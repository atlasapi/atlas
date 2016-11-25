package org.atlasapi.equiv.handlers;

import org.atlasapi.equiv.results.EquivalenceResult;
import org.atlasapi.equiv.results.persistence.EquivalenceResultStore;
import org.atlasapi.media.entity.Content;

public class ResultWritingEquivalenceHandler<T extends Content>
        implements EquivalenceResultHandler<T> {

    private EquivalenceResultStore store;

    public ResultWritingEquivalenceHandler(EquivalenceResultStore store) {
        this.store = store;
    }
    
    @Override
    public boolean handle(EquivalenceResult<T> result) {
        store.store(result);
        return false;
    }
}

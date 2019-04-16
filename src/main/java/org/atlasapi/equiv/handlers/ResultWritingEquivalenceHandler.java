package org.atlasapi.equiv.handlers;

import org.atlasapi.equiv.results.EquivalenceResults;
import org.atlasapi.equiv.results.persistence.EquivalenceResultStore;
import org.atlasapi.media.entity.Content;

public class ResultWritingEquivalenceHandler<T extends Content>
        implements EquivalenceResultHandler<T> {

    private EquivalenceResultStore store;

    public ResultWritingEquivalenceHandler(EquivalenceResultStore store) {
        this.store = store;
    }
    
    @Override
    public boolean handle(EquivalenceResults<T> results) {
        store.store(results);
        return false;
    }
}

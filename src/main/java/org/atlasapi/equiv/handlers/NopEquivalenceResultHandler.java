package org.atlasapi.equiv.handlers;

import org.atlasapi.equiv.results.EquivalenceResults;
import org.atlasapi.media.entity.Content;

public class NopEquivalenceResultHandler<T extends Content> implements EquivalenceResultHandler<T> {

    public NopEquivalenceResultHandler() {
    }

    @Override
    public boolean handle(EquivalenceResults<T> results) {
        return false;
    }
}

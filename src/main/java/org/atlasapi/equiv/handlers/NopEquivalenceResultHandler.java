package org.atlasapi.equiv.handlers;

import org.atlasapi.equiv.results.EquivalenceResult;
import org.atlasapi.media.entity.Content;

public class NopEquivalenceResultHandler<T extends Content> implements EquivalenceResultHandler<T> {

    public NopEquivalenceResultHandler() {
    }

    @Override
    public boolean handle(EquivalenceResult<T> result) {
        return false;
    }
}

package org.atlasapi.equiv.handlers;

import org.atlasapi.equiv.results.EquivalenceResult;

public interface EquivalenceResultHandler<T> {

    void handle(
            EquivalenceResult<T> result
    );
    
}

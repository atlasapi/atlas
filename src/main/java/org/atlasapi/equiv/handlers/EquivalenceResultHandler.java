package org.atlasapi.equiv.handlers;

import java.util.Optional;

import org.atlasapi.equiv.results.EquivalenceResult;

public interface EquivalenceResultHandler<T> {

    void handle(
            EquivalenceResult<T> result,
            Optional<String> taskId
    );
    
}

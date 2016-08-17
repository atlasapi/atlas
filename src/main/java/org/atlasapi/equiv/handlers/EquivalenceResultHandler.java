package org.atlasapi.equiv.handlers;

import java.util.Optional;

import org.atlasapi.equiv.results.EquivalenceResult;

import com.metabroadcast.columbus.telescope.client.IngestTelescopeClientImpl;

public interface EquivalenceResultHandler<T> {

    void handle(EquivalenceResult<T> result);

    void handleWithReporting(
            EquivalenceResult<T> result,
            Optional<String> taskId,
            IngestTelescopeClientImpl telescopeClient
    );
    
}

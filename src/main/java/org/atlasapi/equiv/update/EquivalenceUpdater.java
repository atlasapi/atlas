package org.atlasapi.equiv.update;

import org.atlasapi.equiv.update.metadata.EquivalenceUpdaterMetadata;

import java.util.Optional;

import com.metabroadcast.columbus.telescope.client.IngestTelescopeClientImpl;

public interface EquivalenceUpdater<T> {

    boolean updateEquivalences(
            T subject,
            Optional<String> taskId,
            IngestTelescopeClientImpl telescopeClient
    );
    
    boolean updateEquivalences(T subject);

    EquivalenceUpdaterMetadata getMetadata();
}

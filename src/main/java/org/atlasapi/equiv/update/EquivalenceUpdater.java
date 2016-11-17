package org.atlasapi.equiv.update;

import java.util.Optional;

import org.atlasapi.equiv.update.metadata.EquivalenceUpdaterMetadata;

import com.metabroadcast.columbus.telescope.client.IngestTelescopeClientImpl;

public interface EquivalenceUpdater<T> {

    boolean updateEquivalences(
            T subject,
            Optional<String> taskId,
            IngestTelescopeClientImpl telescopeClient
    );

    EquivalenceUpdaterMetadata getMetadata();
}

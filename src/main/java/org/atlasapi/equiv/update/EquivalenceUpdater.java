package org.atlasapi.equiv.update;

import java.util.Optional;
import java.util.Set;

import org.atlasapi.equiv.update.metadata.EquivalenceUpdaterMetadata;
import org.atlasapi.media.entity.Publisher;

import com.metabroadcast.columbus.telescope.client.IngestTelescopeClientImpl;

public interface EquivalenceUpdater<T> {

    boolean updateEquivalences(
            T subject,
            Optional<String> taskId,
            IngestTelescopeClientImpl telescopeClient
    );

    EquivalenceUpdaterMetadata getMetadata(Set<Publisher> sources);
}

package org.atlasapi.equiv.update;

import java.util.Set;

import org.atlasapi.equiv.update.metadata.EquivalenceUpdaterMetadata;
import org.atlasapi.media.entity.Publisher;

public interface EquivalenceUpdater<T> {

    boolean updateEquivalences(T subject);

    EquivalenceUpdaterMetadata getMetadata(Set<Publisher> sources);
}

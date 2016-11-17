package org.atlasapi.equiv.update;

import org.atlasapi.equiv.update.metadata.EquivalenceUpdaterMetadata;

public interface EquivalenceUpdater<T> {

    boolean updateEquivalences(T subject);

    EquivalenceUpdaterMetadata getMetadata();
}

package org.atlasapi.equiv.update;

import java.util.Set;

import org.atlasapi.equiv.update.metadata.EquivalenceUpdaterMetadata;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.reporting.telescope.OwlTelescopeReporter;


public interface EquivalenceUpdater<T> {

    boolean updateEquivalences(T subject, OwlTelescopeReporter telescopeProxy);

    EquivalenceUpdaterMetadata getMetadata(Set<Publisher> sources);
}

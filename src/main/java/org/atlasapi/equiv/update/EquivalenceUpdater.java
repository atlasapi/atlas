package org.atlasapi.equiv.update;

import java.util.Set;

import org.atlasapi.equiv.update.metadata.EquivalenceUpdaterMetadata;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.reporting.telescope.OwlTelescopeProxy;
import org.atlasapi.reporting.telescope.TelescopeProxy;

public interface EquivalenceUpdater<T> {

    boolean updateEquivalences(T subject, OwlTelescopeProxy telescopeProxy);

    EquivalenceUpdaterMetadata getMetadata(Set<Publisher> sources);
}

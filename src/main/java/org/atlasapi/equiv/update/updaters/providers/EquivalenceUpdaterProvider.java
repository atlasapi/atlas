package org.atlasapi.equiv.update.updaters.providers;

import java.util.Set;

import org.atlasapi.equiv.update.EquivalenceUpdater;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Publisher;

public interface EquivalenceUpdaterProvider<T extends Content> {

    EquivalenceUpdater<T> getUpdater(
            EquivalenceUpdaterProviderDependencies dependencies,
            Set<Publisher> targetPublishers
    );
}

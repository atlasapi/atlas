package org.atlasapi.equiv.update.updaters.providers;

import org.atlasapi.equiv.update.EquivalenceResultUpdater;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Publisher;

import java.util.Set;

public interface EquivalenceResultUpdaterProvider<T extends Content> {

    EquivalenceResultUpdater<T> getUpdater(
            EquivalenceUpdaterProviderDependencies dependencies,
            Set<Publisher> targetPublishers
    );
}

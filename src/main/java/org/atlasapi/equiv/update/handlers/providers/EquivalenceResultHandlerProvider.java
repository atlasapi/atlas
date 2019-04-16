package org.atlasapi.equiv.update.handlers.providers;

import org.atlasapi.equiv.handlers.EquivalenceResultHandler;
import org.atlasapi.equiv.update.updaters.providers.EquivalenceUpdaterProviderDependencies;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Publisher;

import java.util.Set;

public interface EquivalenceResultHandlerProvider<T extends Content> {
    EquivalenceResultHandler<T> getHandler(
            EquivalenceUpdaterProviderDependencies dependencies,
            Set<Publisher> targetPublishers
    );
}

package org.atlasapi.equiv.update.messagers.providers;

import org.atlasapi.equiv.messengers.EquivalenceResultMessenger;
import org.atlasapi.equiv.update.updaters.providers.EquivalenceUpdaterProviderDependencies;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Publisher;

import java.util.Set;

public interface EquivalenceResultMessengerProvider<T extends Content> {
    EquivalenceResultMessenger<T> getMessenger(
            EquivalenceUpdaterProviderDependencies dependencies,
            Set<Publisher> targetPublishers
    );
}

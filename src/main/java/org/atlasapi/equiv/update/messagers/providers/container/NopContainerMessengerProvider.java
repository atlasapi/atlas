package org.atlasapi.equiv.update.messagers.providers.container;

import org.atlasapi.equiv.messengers.EquivalenceResultMessenger;
import org.atlasapi.equiv.messengers.NopEquivalenceResultMessenger;
import org.atlasapi.equiv.update.messagers.providers.EquivalenceResultMessengerProvider;
import org.atlasapi.equiv.update.updaters.providers.EquivalenceUpdaterProviderDependencies;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Publisher;

import java.util.Set;

public class NopContainerMessengerProvider implements EquivalenceResultMessengerProvider<Container> {

    private NopContainerMessengerProvider() {
    }

    public static NopContainerMessengerProvider create() {
        return new NopContainerMessengerProvider();
    }

    @Override
    public EquivalenceResultMessenger<Container> getMessenger(
            EquivalenceUpdaterProviderDependencies dependencies,
            Set<Publisher> targetPublishers
    ) {
        return new NopEquivalenceResultMessenger<>();
    }
}

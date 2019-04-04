package org.atlasapi.equiv.update.messagers.providers.container;

import org.atlasapi.equiv.messengers.EquivalenceResultMessenger;
import org.atlasapi.equiv.messengers.QueueingEquivalenceResultMessenger;
import org.atlasapi.equiv.update.messagers.providers.EquivalenceResultMessengerProvider;
import org.atlasapi.equiv.update.updaters.providers.EquivalenceUpdaterProviderDependencies;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Publisher;

import java.util.Set;

public class StandardTopLevelContainerMessengerProvider implements EquivalenceResultMessengerProvider<Container> {

    private StandardTopLevelContainerMessengerProvider() {
    }

    public static StandardTopLevelContainerMessengerProvider create() {
        return new StandardTopLevelContainerMessengerProvider();
    }

    @Override
    public EquivalenceResultMessenger<Container> getMessenger(
            EquivalenceUpdaterProviderDependencies dependencies,
            Set<Publisher> targetPublishers
    ) {
        return QueueingEquivalenceResultMessenger.create(
                dependencies.getMessageSender(),
                dependencies.getLookupEntryStore()
        );
    }
}

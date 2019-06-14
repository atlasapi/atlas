package org.atlasapi.equiv.update.messagers.providers.container;

import org.atlasapi.equiv.messengers.EquivalenceResultMessenger;
import org.atlasapi.equiv.messengers.QueueingEquivalenceResultMessenger;
import org.atlasapi.equiv.update.messagers.providers.EquivalenceResultMessengerProvider;
import org.atlasapi.equiv.update.updaters.providers.EquivalenceUpdaterProviderDependencies;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Publisher;

import java.util.Set;

public class StandardSeriesMessengerProvider implements EquivalenceResultMessengerProvider<Container> {

    private StandardSeriesMessengerProvider() {
    }

    public static StandardSeriesMessengerProvider create() {
        return new StandardSeriesMessengerProvider();
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

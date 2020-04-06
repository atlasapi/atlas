package org.atlasapi.equiv.update.messagers.providers.item;

import org.atlasapi.equiv.messengers.EquivalenceResultMessenger;
import org.atlasapi.equiv.messengers.NopEquivalenceResultMessenger;
import org.atlasapi.equiv.update.messagers.providers.EquivalenceResultMessengerProvider;
import org.atlasapi.equiv.update.updaters.providers.EquivalenceUpdaterProviderDependencies;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;

import java.util.Set;

public class StandardItemMessengerProvider implements EquivalenceResultMessengerProvider<Item> {

    private StandardItemMessengerProvider() {
    }

    public static StandardItemMessengerProvider create() {
        return new StandardItemMessengerProvider();
    }

    @Override
    public EquivalenceResultMessenger<Item> getMessenger(
            EquivalenceUpdaterProviderDependencies dependencies,
            Set<Publisher> targetPublishers
    ) {
        // This used to be a QueueingEquivalenceResultMessenger but this has been disabled due to the messaging being
        // moved to the TransitiveLookupWriter. Framework has been kept intact for possible future use.
        return new NopEquivalenceResultMessenger<>();
    }
}

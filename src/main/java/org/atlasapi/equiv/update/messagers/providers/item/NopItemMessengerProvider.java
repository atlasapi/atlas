package org.atlasapi.equiv.update.messagers.providers.item;

import org.atlasapi.equiv.messengers.EquivalenceResultMessenger;
import org.atlasapi.equiv.messengers.NopEquivalenceResultMessenger;
import org.atlasapi.equiv.update.messagers.providers.EquivalenceResultMessengerProvider;
import org.atlasapi.equiv.update.updaters.providers.EquivalenceUpdaterProviderDependencies;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;

import java.util.Set;

public class NopItemMessengerProvider implements EquivalenceResultMessengerProvider<Item> {

    private NopItemMessengerProvider() {
    }

    public static NopItemMessengerProvider create() {
        return new NopItemMessengerProvider();
    }

    @Override
    public EquivalenceResultMessenger<Item> getMessenger(
            EquivalenceUpdaterProviderDependencies dependencies,
            Set<Publisher> targetPublishers
    ) {
        return new NopEquivalenceResultMessenger<>();
    }
}

package org.atlasapi.equiv.update.messagers.types;

import org.atlasapi.equiv.update.messagers.providers.EquivalenceResultMessengerProvider;
import org.atlasapi.equiv.update.messagers.providers.item.NopItemMessengerProvider;
import org.atlasapi.equiv.update.messagers.providers.item.StandardItemMessengerProvider;
import org.atlasapi.media.entity.Item;

import static com.google.common.base.Preconditions.checkNotNull;

public enum ItemEquivalenceMessengerType {
    NOP_ITEM(
            NopItemMessengerProvider.create()
    ),
    STANDARD_ITEM(
            StandardItemMessengerProvider.create()
    ),
    ;

    private final EquivalenceResultMessengerProvider<Item> messengerProvider;

    ItemEquivalenceMessengerType(EquivalenceResultMessengerProvider<Item> messengerProvider) {
        this.messengerProvider = checkNotNull(messengerProvider);
    }

    public EquivalenceResultMessengerProvider<Item> getMessengerProvider() {
        return messengerProvider;
    }
}

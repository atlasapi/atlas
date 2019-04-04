package org.atlasapi.equiv.update.handlers.types;

import org.atlasapi.equiv.update.handlers.providers.EquivalenceResultHandlerProvider;
import org.atlasapi.equiv.update.handlers.providers.item.NopItemHandlerProvider;
import org.atlasapi.equiv.update.handlers.providers.item.StandardItemHandlerProvider;
import org.atlasapi.equiv.update.handlers.providers.item.StrictEpisodeItemHandlerProvider;
import org.atlasapi.media.entity.Item;

import static com.google.common.base.Preconditions.checkNotNull;

public enum ItemEquivalenceHandlerType {
    NOP_ITEM(
            NopItemHandlerProvider.create()
    ),
    STANDARD_ITEM(
            StandardItemHandlerProvider.create()
    ),
    STRICT_EPISODE_ITEM(
            StrictEpisodeItemHandlerProvider.create()
    )
    ;

    private final EquivalenceResultHandlerProvider<Item> handlerProvider;

    ItemEquivalenceHandlerType(EquivalenceResultHandlerProvider<Item> handlerProvider) {
        this.handlerProvider = checkNotNull(handlerProvider);
    }

    public EquivalenceResultHandlerProvider<Item> getHandlerProvider() {
        return handlerProvider;
    }
}

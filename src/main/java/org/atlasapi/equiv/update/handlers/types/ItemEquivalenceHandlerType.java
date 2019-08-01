package org.atlasapi.equiv.update.handlers.types;

import org.atlasapi.equiv.update.handlers.providers.EquivalenceResultHandlerProvider;
import org.atlasapi.equiv.update.handlers.providers.item.NopItemHandlerProvider;
import org.atlasapi.equiv.update.handlers.providers.item.StandardItemHandlerProvider;
import org.atlasapi.equiv.update.handlers.providers.item.StrictEpisodeItemHandlerProvider;
import org.atlasapi.media.entity.Item;

import static com.google.common.base.Preconditions.checkNotNull;

public enum ItemEquivalenceHandlerType {
    NOP_ITEM_HANDLER(
            NopItemHandlerProvider.create()
    ),
    STANDARD_ITEM_HANDLER(
            StandardItemHandlerProvider.create()
    ),
    STRICT_EPISODE_ITEM_HANDLER(
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

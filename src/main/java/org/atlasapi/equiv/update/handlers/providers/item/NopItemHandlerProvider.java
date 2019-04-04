package org.atlasapi.equiv.update.handlers.providers.item;

import org.atlasapi.equiv.handlers.EquivalenceResultHandler;
import org.atlasapi.equiv.results.EquivalenceResult;
import org.atlasapi.equiv.update.handlers.providers.EquivalenceResultHandlerProvider;
import org.atlasapi.equiv.update.updaters.providers.EquivalenceUpdaterProviderDependencies;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;

import java.util.Set;

public class NopItemHandlerProvider implements EquivalenceResultHandlerProvider<Item> {

    private NopItemHandlerProvider() {

    }

    public static NopItemHandlerProvider create() {
        return new NopItemHandlerProvider();
    }

    @Override
    public EquivalenceResultHandler<Item> getHandler(
            EquivalenceUpdaterProviderDependencies dependencies,
            Set<Publisher> targetPublishers
    ) {
        return new EquivalenceResultHandler<Item>() {
            @Override
            public boolean handle(EquivalenceResult<Item> result) {
                return false;
            }
        };
    }
}

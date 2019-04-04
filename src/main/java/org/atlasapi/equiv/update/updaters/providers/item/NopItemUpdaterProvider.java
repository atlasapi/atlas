package org.atlasapi.equiv.update.updaters.providers.item;

import java.util.Set;

import org.atlasapi.equiv.results.EquivalenceResult;
import org.atlasapi.equiv.update.EquivalenceResultUpdater;
import org.atlasapi.equiv.update.EquivalenceUpdater;
import org.atlasapi.equiv.update.metadata.EquivalenceUpdaterMetadata;
import org.atlasapi.equiv.update.metadata.NopEquivalenceUpdaterMetadata;
import org.atlasapi.equiv.update.updaters.providers.EquivalenceResultUpdaterProvider;
import org.atlasapi.equiv.update.updaters.providers.EquivalenceUpdaterProvider;
import org.atlasapi.equiv.update.updaters.providers.EquivalenceUpdaterProviderDependencies;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.reporting.telescope.OwlTelescopeReporter;

public class NopItemUpdaterProvider implements EquivalenceResultUpdaterProvider<Item> {

    private NopItemUpdaterProvider() {
    }

    public static NopItemUpdaterProvider create() {
        return new NopItemUpdaterProvider();
    }

    //TODO: return meaningful objects
    @Override
    public EquivalenceResultUpdater<Item> getUpdater(
            EquivalenceUpdaterProviderDependencies dependencies,
            Set<Publisher> targetPublishers
    ) {
        return new EquivalenceResultUpdater<Item>() {
            @Override
            public EquivalenceResult<Item> provideEquivalenceResult(Item subject, OwlTelescopeReporter telescope) {
                return null;
            }

            @Override
            public EquivalenceUpdaterMetadata getMetadata() {
                return null;
            }
        };
    }
}

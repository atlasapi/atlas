package org.atlasapi.equiv.update.updaters.providers.item;

import java.util.Set;

import org.atlasapi.equiv.update.EquivalenceUpdater;
import org.atlasapi.equiv.update.metadata.EquivalenceUpdaterMetadata;
import org.atlasapi.equiv.update.metadata.NopEquivalenceUpdaterMetadata;
import org.atlasapi.equiv.update.updaters.providers.EquivalenceUpdaterProvider;
import org.atlasapi.equiv.update.updaters.providers.EquivalenceUpdaterProviderDependencies;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.reporting.telescope.OwlTelescopeReporter;

public class NopItemUpdaterProvider implements EquivalenceUpdaterProvider<Item> {

    private NopItemUpdaterProvider() {
    }

    public static NopItemUpdaterProvider create() {
        return new NopItemUpdaterProvider();
    }

    @Override
    public EquivalenceUpdater<Item> getUpdater(
            EquivalenceUpdaterProviderDependencies dependencies,
            Set<Publisher> targetPublishers
    ) {
        return new EquivalenceUpdater<Item>() {

            @Override
            public boolean updateEquivalences(Item subject, OwlTelescopeReporter telescopeProxy) {
                return false;
            }

            @Override
            public EquivalenceUpdaterMetadata getMetadata(Set<Publisher> sources) {
                return NopEquivalenceUpdaterMetadata.create();
            }
        };
    }
}

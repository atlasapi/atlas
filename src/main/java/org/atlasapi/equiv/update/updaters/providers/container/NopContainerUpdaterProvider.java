package org.atlasapi.equiv.update.updaters.providers.container;

import java.util.Set;

import org.atlasapi.equiv.update.EquivalenceUpdater;
import org.atlasapi.equiv.update.metadata.EquivalenceUpdaterMetadata;
import org.atlasapi.equiv.update.metadata.NopEquivalenceUpdaterMetadata;
import org.atlasapi.equiv.update.updaters.providers.EquivalenceUpdaterProvider;
import org.atlasapi.equiv.update.updaters.providers.EquivalenceUpdaterProviderDependencies;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Publisher;

public class NopContainerUpdaterProvider implements EquivalenceUpdaterProvider<Container> {

    private NopContainerUpdaterProvider() {
    }

    public static NopContainerUpdaterProvider create() {
        return new NopContainerUpdaterProvider();
    }

    @Override
    public EquivalenceUpdater<Container> getUpdater(
            EquivalenceUpdaterProviderDependencies dependencies,
            Set<Publisher> targetPublishers
    ) {
        return new EquivalenceUpdater<Container>() {

            @Override
            public boolean updateEquivalences(Container subject) {
                return false;
            }

            @Override
            public EquivalenceUpdaterMetadata getMetadata(Set<Publisher> sources) {
                return NopEquivalenceUpdaterMetadata.create();
            }
        };
    }
}

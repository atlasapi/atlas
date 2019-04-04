package org.atlasapi.equiv.update.updaters.providers.container;

import java.util.Set;

import org.atlasapi.equiv.results.EquivalenceResult;
import org.atlasapi.equiv.update.EquivalenceResultUpdater;
import org.atlasapi.equiv.update.EquivalenceUpdater;
import org.atlasapi.equiv.update.metadata.EquivalenceUpdaterMetadata;
import org.atlasapi.equiv.update.metadata.NopEquivalenceUpdaterMetadata;
import org.atlasapi.equiv.update.updaters.providers.EquivalenceResultUpdaterProvider;
import org.atlasapi.equiv.update.updaters.providers.EquivalenceUpdaterProvider;
import org.atlasapi.equiv.update.updaters.providers.EquivalenceUpdaterProviderDependencies;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.reporting.telescope.OwlTelescopeReporter;

public class NopContainerUpdaterProvider implements EquivalenceResultUpdaterProvider<Container> {

    private NopContainerUpdaterProvider() {
    }

    public static NopContainerUpdaterProvider create() {
        return new NopContainerUpdaterProvider();
    }

    //TODO return meaningful objects
    @Override
    public EquivalenceResultUpdater<Container> getUpdater(
            EquivalenceUpdaterProviderDependencies dependencies,
            Set<Publisher> targetPublishers
    ) {
        return new EquivalenceResultUpdater<Container>() {
            @Override
            public EquivalenceResult<Container> provideEquivalenceResult(Container subject, OwlTelescopeReporter telescope) {
                return null;
            }

            @Override
            public EquivalenceUpdaterMetadata getMetadata() {
                return null;
            }
        };
    }
}

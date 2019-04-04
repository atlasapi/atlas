package org.atlasapi.equiv.update.handlers.providers.container;

import org.atlasapi.equiv.handlers.EquivalenceResultHandler;
import org.atlasapi.equiv.results.EquivalenceResult;
import org.atlasapi.equiv.update.handlers.providers.EquivalenceResultHandlerProvider;
import org.atlasapi.equiv.update.updaters.providers.EquivalenceUpdaterProviderDependencies;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Publisher;

import java.util.Set;

public class NopContainerHandlerProvider implements EquivalenceResultHandlerProvider<Container> {

    private NopContainerHandlerProvider() {
    }

    public static NopContainerHandlerProvider create() {
        return new NopContainerHandlerProvider();
    }

    @Override
    public EquivalenceResultHandler<Container> getHandler(
            EquivalenceUpdaterProviderDependencies dependencies,
            Set<Publisher> targetPublishers
    ) {
        return new EquivalenceResultHandler<Container>() {
            @Override
            public boolean handle(EquivalenceResult<Container> result) {
                return false;
            }
        };
    }
}

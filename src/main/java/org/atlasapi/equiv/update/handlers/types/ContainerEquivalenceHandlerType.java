package org.atlasapi.equiv.update.handlers.types;

import org.atlasapi.equiv.update.handlers.providers.EquivalenceResultHandlerProvider;
import org.atlasapi.equiv.update.handlers.providers.container.NopContainerHandlerProvider;
import org.atlasapi.equiv.update.handlers.providers.container.StandardSeriesHandlerProvider;
import org.atlasapi.equiv.update.handlers.providers.container.StandardTopLevelContainerHandlerProvider;
import org.atlasapi.media.entity.Container;

import static com.google.common.base.Preconditions.checkNotNull;

public enum ContainerEquivalenceHandlerType {
    NOP_CONTAINER(
            NopContainerHandlerProvider.create()
    ),
    STANDARD_TOP_LEVEL_CONTAINER(
            StandardTopLevelContainerHandlerProvider.create()
    ),
    STANDARD_SERIES(
            StandardSeriesHandlerProvider.create()
    )
    ;

    private final EquivalenceResultHandlerProvider<Container> handlerProvider;

    ContainerEquivalenceHandlerType(EquivalenceResultHandlerProvider<Container> handlerProvider) {
        this.handlerProvider = checkNotNull(handlerProvider);
    }

    public EquivalenceResultHandlerProvider<Container> getHandlerProvider() {
        return handlerProvider;
    }
}

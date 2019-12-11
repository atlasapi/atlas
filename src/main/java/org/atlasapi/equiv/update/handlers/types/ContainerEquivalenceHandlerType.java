package org.atlasapi.equiv.update.handlers.types;

import org.atlasapi.equiv.update.handlers.providers.EquivalenceResultHandlerProvider;
import org.atlasapi.equiv.update.handlers.providers.container.NopContainerHandlerProvider;
import org.atlasapi.equiv.update.handlers.providers.container.StandardNoEpisodeMatchingContainerHandlerProvider;
import org.atlasapi.equiv.update.handlers.providers.container.StandardTopLevelContainerHandlerProvider;
import org.atlasapi.media.entity.Container;

import static com.google.common.base.Preconditions.checkNotNull;

public enum ContainerEquivalenceHandlerType {
    NOP_CONTAINER_HANDLER(
            NopContainerHandlerProvider.create()
    ),
    STANDARD_CONTAINER_HANDLER(
            StandardTopLevelContainerHandlerProvider.create()
    ),
    STANDARD_SERIES_HANDLER(
            StandardNoEpisodeMatchingContainerHandlerProvider.create()
    ),
    STANDARD_NO_EPISODE_MATCHING_CONTAINER_HANDLER(
            StandardNoEpisodeMatchingContainerHandlerProvider.create()
    ),
    ;

    private final EquivalenceResultHandlerProvider<Container> handlerProvider;

    ContainerEquivalenceHandlerType(EquivalenceResultHandlerProvider<Container> handlerProvider) {
        this.handlerProvider = checkNotNull(handlerProvider);
    }

    public EquivalenceResultHandlerProvider<Container> getHandlerProvider() {
        return handlerProvider;
    }
}

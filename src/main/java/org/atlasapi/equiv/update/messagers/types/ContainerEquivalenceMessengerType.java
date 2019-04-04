package org.atlasapi.equiv.update.messagers.types;

import org.atlasapi.equiv.update.messagers.providers.EquivalenceResultMessengerProvider;
import org.atlasapi.equiv.update.messagers.providers.container.NopContainerMessengerProvider;
import org.atlasapi.equiv.update.messagers.providers.container.StandardSeriesMessengerProvider;
import org.atlasapi.equiv.update.messagers.providers.container.StandardTopLevelContainerMessengerProvider;
import org.atlasapi.media.entity.Container;

import static com.google.common.base.Preconditions.checkNotNull;

public enum ContainerEquivalenceMessengerType {
    NOP_CONTAINER(
            NopContainerMessengerProvider.create()
    ),
    STANDARD_TOP_LEVEL_CONTAINER(
            StandardTopLevelContainerMessengerProvider.create()
    ),
    STANDARD_SERIES(
            StandardSeriesMessengerProvider.create()
    )

    ;

    private final EquivalenceResultMessengerProvider<Container> messengerProvider;

    ContainerEquivalenceMessengerType(EquivalenceResultMessengerProvider<Container> messengerProvider) {
        this.messengerProvider = checkNotNull(messengerProvider);
    }

    public EquivalenceResultMessengerProvider<Container> getMessengerProvider() {
        return messengerProvider;
    }
}

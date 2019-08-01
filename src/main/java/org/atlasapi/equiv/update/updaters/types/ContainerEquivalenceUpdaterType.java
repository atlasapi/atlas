package org.atlasapi.equiv.update.updaters.types;

import org.atlasapi.equiv.update.updaters.providers.EquivalenceResultUpdaterProvider;
import org.atlasapi.equiv.update.updaters.providers.container.AmazonContainerUpdaterProvider;
import org.atlasapi.equiv.update.updaters.providers.container.AmazonSeriesUpdaterProvider;
import org.atlasapi.equiv.update.updaters.providers.container.BroadcastItemContainerUpdaterProvider;
import org.atlasapi.equiv.update.updaters.providers.container.BtVodContainerUpdaterProvider;
import org.atlasapi.equiv.update.updaters.providers.container.FacebookContainerUpdaterProvider;
import org.atlasapi.equiv.update.updaters.providers.container.ImdbApiContainerUpdateProvider;
import org.atlasapi.equiv.update.updaters.providers.container.NopContainerUpdaterProvider;
import org.atlasapi.equiv.update.updaters.providers.container.RtUpcomingContainerUpdaterProvider;
import org.atlasapi.equiv.update.updaters.providers.container.RteContainerUpdaterProvider;
import org.atlasapi.equiv.update.updaters.providers.container.StandardSeriesUpdaterProvider;
import org.atlasapi.equiv.update.updaters.providers.container.StandardTopLevelContainerUpdaterProvider;
import org.atlasapi.equiv.update.updaters.providers.container.VodContainerUpdaterProvider;
import org.atlasapi.equiv.update.updaters.providers.container.WikipediaContainerUpdateProvider;
import org.atlasapi.media.entity.Container;

import static com.google.common.base.Preconditions.checkNotNull;

public enum ContainerEquivalenceUpdaterType {
    NOP_CONTAINER(
            NopContainerUpdaterProvider.create()
    ),
    STANDARD_TOP_LEVEL_CONTAINER(
            StandardTopLevelContainerUpdaterProvider.create()
    ),
    STANDARD_SERIES(
            StandardSeriesUpdaterProvider.create()
    ),
    RT_UPCOMING_CONTAINER(
            RtUpcomingContainerUpdaterProvider.create()
    ),
    BROADCAST_ITEM_CONTAINER(
            BroadcastItemContainerUpdaterProvider.create()
    ),
    FACEBOOK_CONTAINER(
            FacebookContainerUpdaterProvider.create()
    ),
    VOD_CONTAINER(
            VodContainerUpdaterProvider.create()
    ),
    BT_VOD_CONTAINER(
            BtVodContainerUpdaterProvider.create()
    ),
    RTE_VOD_CONTAINER(
            RteContainerUpdaterProvider.create()
    ),
    AMAZON_CONTAINER(
            AmazonContainerUpdaterProvider.create()
    ),
    AMAZON_SERIES(
            AmazonSeriesUpdaterProvider.create()
    ),
    WIKIPEDIA_CONTAINER(
            WikipediaContainerUpdateProvider.create()
    ),
    IMDB_API_CONTAINER(
            ImdbApiContainerUpdateProvider.create()
    ),
    ;

    private final EquivalenceResultUpdaterProvider<Container> provider;

    ContainerEquivalenceUpdaterType(EquivalenceResultUpdaterProvider<Container> provider) {
        this.provider = checkNotNull(provider);
    }

    public EquivalenceResultUpdaterProvider<Container> getProvider() {
        return provider;
    }
}

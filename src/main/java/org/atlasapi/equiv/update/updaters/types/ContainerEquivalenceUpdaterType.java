package org.atlasapi.equiv.update.updaters.types;

import org.atlasapi.equiv.update.updaters.providers.EquivalenceResultUpdaterProvider;
import org.atlasapi.equiv.update.updaters.providers.container.AliasContainerUpdaterProvider;
import org.atlasapi.equiv.update.updaters.providers.container.BroadcastItemContainerUpdaterProvider;
import org.atlasapi.equiv.update.updaters.providers.container.BtVodContainerUpdaterProvider;
import org.atlasapi.equiv.update.updaters.providers.container.FacebookContainerUpdaterProvider;
import org.atlasapi.equiv.update.updaters.providers.container.NopContainerUpdaterProvider;
import org.atlasapi.equiv.update.updaters.providers.container.RtUpcomingContainerUpdaterProvider;
import org.atlasapi.equiv.update.updaters.providers.container.RteContainerUpdaterProvider;
import org.atlasapi.equiv.update.updaters.providers.container.StandardSeriesUpdaterProvider;
import org.atlasapi.equiv.update.updaters.providers.container.StandardTopLevelContainerUpdaterProvider;
import org.atlasapi.equiv.update.updaters.providers.container.VodContainerUpdaterProvider;
import org.atlasapi.equiv.update.updaters.providers.container.WikipediaContainerUpdateProvider;
import org.atlasapi.equiv.update.updaters.providers.container.amazon.AmazonContainerUpdaterProvider;
import org.atlasapi.equiv.update.updaters.providers.container.amazon.AmazonSeriesUpdaterProvider;
import org.atlasapi.equiv.update.updaters.providers.container.amazon.AmazonToAmazonContainerUpdaterProvider;
import org.atlasapi.equiv.update.updaters.providers.container.amazon.AmazonToAmazonSeriesUpdaterProvider;
import org.atlasapi.equiv.update.updaters.providers.container.imdb.ImdbContainerUpdaterProvider;
import org.atlasapi.equiv.update.updaters.providers.container.imdb.ImdbPaContainerUpdaterProvider;
import org.atlasapi.equiv.update.updaters.providers.container.imdb.ImdbPaSeriesUpdaterProvider;
import org.atlasapi.equiv.update.updaters.providers.container.imdb.ImdbSeriesUpdaterProvider;
import org.atlasapi.equiv.utils.imdb.ImdbEquivUtils;
import org.atlasapi.equiv.utils.pa.PaApiEquivUtils;
import org.atlasapi.media.entity.Container;

import com.google.common.collect.ImmutableSet;

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
    AMAZON_AMAZON_CONTAINER(
            AmazonToAmazonContainerUpdaterProvider.create()
    ),
    AMAZON_CONTAINER(
            AmazonContainerUpdaterProvider.create()
    ),
    AMAZON_AMAZON_SERIES(
            AmazonToAmazonSeriesUpdaterProvider.create()
    ),
    AMAZON_SERIES(
            AmazonSeriesUpdaterProvider.create()
    ),
    WIKIPEDIA_CONTAINER(
            WikipediaContainerUpdateProvider.create()
    ),
    IMDB_CONTAINER(
            ImdbContainerUpdaterProvider.create(
                    ImmutableSet.of(
                            ImdbEquivUtils.IMDB_ALIAS_NAMESPACES
                    ))
    ),
    IMDB_PA_CONTAINER(
            ImdbPaContainerUpdaterProvider.create()
    ),
    IMDB_SERIES(
            ImdbSeriesUpdaterProvider.create()
    ),
    IMDB_PA_SERIES(
            ImdbPaSeriesUpdaterProvider.create()
    ),
    PA_API_CONTAINER(
            AliasContainerUpdaterProvider.create(
                    ImmutableSet.of(
                            PaApiEquivUtils.CONTAINER_LEGACY_PA_ID_ALIAS_NAMESPACES
                    ))
    ),
    PA_API_SERIES(
            AliasContainerUpdaterProvider.create(
                    ImmutableSet.of(
                            PaApiEquivUtils.SERIES_LEGACY_PA_ID_ALIAS_NAMESPACES
                    ))
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

package org.atlasapi.equiv.update.updaters.types;

import com.google.common.collect.ImmutableSet;
import org.atlasapi.equiv.update.updaters.providers.EquivalenceResultUpdaterProvider;
import org.atlasapi.equiv.update.updaters.providers.item.AliasItemUpdaterProvider;
import org.atlasapi.equiv.update.updaters.providers.item.BettyItemUpdaterProvider;
import org.atlasapi.equiv.update.updaters.providers.item.BroadcastItemUpdaterProvider;
import org.atlasapi.equiv.update.updaters.providers.item.BtVodItemUpdaterProvider;
import org.atlasapi.equiv.update.updaters.providers.item.ItemSearchUpdaterProvider;
import org.atlasapi.equiv.update.updaters.providers.item.ItemSequenceUpdaterProvider;
import org.atlasapi.equiv.update.updaters.providers.item.MusicItemUpdaterProvider;
import org.atlasapi.equiv.update.updaters.providers.item.NopItemUpdaterProvider;
import org.atlasapi.equiv.update.updaters.providers.item.RoviItemUpdaterProvider;
import org.atlasapi.equiv.update.updaters.providers.item.RtItemUpdaterProvider;
import org.atlasapi.equiv.update.updaters.providers.item.RtUpcomingItemUpdaterProvider;
import org.atlasapi.equiv.update.updaters.providers.item.StandardItemUpdaterProvider;
import org.atlasapi.equiv.update.updaters.providers.item.StrictStandardUpdaterProvider;
import org.atlasapi.equiv.update.updaters.providers.item.VodItemUpdaterProvider;
import org.atlasapi.equiv.update.updaters.providers.item.VodItemWithSeriesSequenceUpdaterProvider;
import org.atlasapi.equiv.update.updaters.providers.item.WikipediaItemUpdateProvider;
import org.atlasapi.equiv.update.updaters.providers.item.YouviewItemUpdaterProvider;
import org.atlasapi.equiv.update.updaters.providers.item.aenetworks.AeItemUpdaterProvider;
import org.atlasapi.equiv.update.updaters.providers.item.amazon.AmazonItemUpdaterProvider;
import org.atlasapi.equiv.update.updaters.providers.item.amazon.AmazonToAmazonItemUpdaterProvider;
import org.atlasapi.equiv.update.updaters.providers.item.barb.*;
import org.atlasapi.equiv.utils.imdb.ImdbEquivUtils;
import org.atlasapi.media.entity.Item;

import static com.google.common.base.Preconditions.checkNotNull;

public enum ItemEquivalenceUpdaterType {
    NOP_ITEM(
            NopItemUpdaterProvider.create()
    ),
    STANDARD_ITEM(
            StandardItemUpdaterProvider.create()
    ),
    RT_UPCOMING_ITEM(
            RtUpcomingItemUpdaterProvider.create()
    ),
    STRICT_ITEM(
            StrictStandardUpdaterProvider.create()
    ),
    BROADCAST_ITEM(
            BroadcastItemUpdaterProvider.create()
    ),
    ROVI_ITEM(
            RoviItemUpdaterProvider.create()
    ),
    RT_ITEM(
            RtItemUpdaterProvider.create()
    ),
    YOUVIEW_ITEM(
            YouviewItemUpdaterProvider.create()
    ),
    BETTY_ITEM(
            BettyItemUpdaterProvider.create()
    ),
    VOD_ITEM(
            VodItemUpdaterProvider.create()
    ),
    AMAZON_AMAZON_ITEM(
            AmazonToAmazonItemUpdaterProvider.create()
    ),
    AMAZON_ITEM(
            AmazonItemUpdaterProvider.create()
    ),
    VOD_WITH_SERIES_SEQUENCE_ITEM(
            VodItemWithSeriesSequenceUpdaterProvider.create()
    ),
    BT_VOD_ITEM(
            BtVodItemUpdaterProvider.create()
    ),
    MUSIC_ITEM(
            MusicItemUpdaterProvider.create()
    ),
    BARB_ITEM(
            BarbItemUpdaterProvider.create()
    ),
    TXLOGS_ITEM(
            TxlogsItemUpdaterProvider.create()
    ),
    BBC_REGIONAL_TXLOGS_ITEM(
            BbcRegionalTxlogItemUpdaterProvider.create()
    ),
    BBC_TO_TXLOGS_ITEM(
            BbcTxlogsItemUpdaterProvider.create(false)
    ),
    TXLOGS_TO_BBC_ITEM(
            BbcTxlogsItemUpdaterProvider.create(true)
    ),
    AE_TO_TXLOGS_ITEM(
            AeItemUpdaterProvider.create()
    ),
    WIKIPEDIA_ITEM(
            WikipediaItemUpdateProvider.create()
    ),
    BARB_X_ITEM(
            BarbXItemUpdaterProvider.create()
    ),
    IMDB_ITEM(
            AliasItemUpdaterProvider.create(
                    ImmutableSet.of(
                            ImdbEquivUtils.IMDB_ALIAS_NAMESPACES
                    )
            )
    ),
    ITEM_SEQUENCE(
            ItemSequenceUpdaterProvider.create()
    ),
    ITEM_SEARCH(
            ItemSearchUpdaterProvider.create()
    ),
    ;

    private final EquivalenceResultUpdaterProvider<Item> provider;

    ItemEquivalenceUpdaterType(EquivalenceResultUpdaterProvider<Item> provider) {
        this.provider = checkNotNull(provider);
    }

    public EquivalenceResultUpdaterProvider<Item> getProvider() {
        return provider;
    }
}

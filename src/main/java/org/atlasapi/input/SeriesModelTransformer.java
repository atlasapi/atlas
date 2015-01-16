package org.atlasapi.input;

import static com.google.common.base.Preconditions.checkNotNull;

import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Series;
import org.atlasapi.media.entity.simple.Item;
import org.atlasapi.media.entity.simple.SeriesSummary;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;
import org.atlasapi.persistence.topic.TopicStore;
import org.joda.time.DateTime;

import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.time.Clock;

public class SeriesModelTransformer extends ContentModelTransformer<Item, Series> {

    public SeriesModelTransformer(LookupEntryStore lookupStore,
            TopicStore topicStore,
            NumberToShortStringCodec idCodec,
            ClipModelTransformer clipsModelTransformer, Clock clock) {
        super(lookupStore, topicStore, idCodec, clipsModelTransformer, clock);
    }

    @Override protected Series createContentOutput(Item simple, DateTime now) {
        checkNotNull(simple.getUri(),
                "Cannot create a Series from simple Item without URI");
        checkNotNull(simple.getPublisher(),
                "Cannot create a Series from simple Item without a Publisher");
        checkNotNull(simple.getPublisher().getKey(),
                "Cannot create a Series from simple Item without a Publisher key");
        checkNotNull(simple.getSeriesSummary(),
                "Cannot create a series without a SeriesSummary on simple Item");

        SeriesSummary summary = simple.getSeriesSummary();
        Series series = new Series(
                summary.getUri(),
                summary.getCurie(),
                Publisher.fromKey(simple.getPublisher().getKey()).requireValue()
        );
        series.setTotalEpisodes(summary.getTotalEpisodes());
        series.withSeriesNumber(summary.getSeriesNumber());
    }
}

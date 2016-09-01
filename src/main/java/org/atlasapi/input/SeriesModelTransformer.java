package org.atlasapi.input;

import static com.google.common.base.Preconditions.checkNotNull;

import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.ParentRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Series;
import org.atlasapi.media.entity.simple.Playlist;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;
import org.atlasapi.persistence.topic.TopicStore;
import org.joda.time.DateTime;

import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.time.Clock;

public class SeriesModelTransformer extends ContentModelTransformer<Playlist, Series> {

    public SeriesModelTransformer(LookupEntryStore lookupStore,
            TopicStore topicStore,
            NumberToShortStringCodec idCodec,
            ClipModelTransformer clipsModelTransformer, Clock clock) {
        super(lookupStore, topicStore, idCodec, clipsModelTransformer, clock);
    }

    @Override
    protected Series createOutput(Playlist simple) {
        checkNotNull(simple.getUri(),
                "Cannot create a Series from simple Playlist without URI");
        checkNotNull(simple.getPublisher(),
                "Cannot create a Series from simple Playlist without a Publisher");
        checkNotNull(simple.getPublisher().getKey(),
                "Cannot create a Series from simple Playlist without a Publisher key");

        Series series = new Series(
                simple.getUri(),
                simple.getCurie(),
                Publisher.fromKey(simple.getPublisher().getKey()).requireValue()
        );
        return series;

    }

    @Override
    protected Series setFields(Series result, Playlist simple) {
        super.setFields(result, simple);
        result.setTotalEpisodes(simple.getTotalEpisodes());
        result.withSeriesNumber(simple.getSeriesNumber());
        if (simple.getBrandSummary() != null) {
            result.setParentRef(new ParentRef(simple.getBrandSummary().getUri()));
        }
        return result;
    }
}

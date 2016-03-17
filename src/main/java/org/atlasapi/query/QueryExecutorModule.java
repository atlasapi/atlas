package org.atlasapi.query;

import org.atlasapi.input.BrandModelTransformer;
import org.atlasapi.input.ClipModelTransformer;
import org.atlasapi.input.DefaultGsonModelReader;
import org.atlasapi.input.DelegatingModelTransformer;
import org.atlasapi.input.ItemModelTransformer;
import org.atlasapi.input.SegmentModelTransformer;
import org.atlasapi.input.SeriesModelTransformer;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.segment.SegmentWriter;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.schedule.mongo.ScheduleWriter;
import org.atlasapi.persistence.event.EventResolver;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;
import org.atlasapi.persistence.topic.TopicStore;
import org.atlasapi.query.content.ContentWriteExecutor;

import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.time.SystemClock;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static org.atlasapi.persistence.MongoContentPersistenceModule.NON_ID_SETTING_CONTENT_WRITER;

@Configuration
public class QueryExecutorModule {

    private @Autowired @Qualifier(NON_ID_SETTING_CONTENT_WRITER) ContentWriter contentWriter;
    private @Autowired ScheduleWriter scheduleWriter;
    private @Autowired ContentResolver contentResolver;
    private @Autowired ChannelResolver channelResolver;
    private @Autowired @Qualifier("topicStore") TopicStore topicStore;
    private @Autowired LookupEntryStore lookupStore;
    private @Autowired EventResolver eventResolver;
    private @Autowired SegmentWriter segmentWriter;

    @Bean
    public ContentWriteExecutor contentWriteExecutor() {
        return new ContentWriteExecutor(
                new DefaultGsonModelReader(),
                delegatingModelTransformer(),
                contentResolver,
                contentWriter,
                scheduleWriter,
                channelResolver,
                eventResolver
        );
    }

    private DelegatingModelTransformer delegatingModelTransformer() {
        return new DelegatingModelTransformer(
                brandTransformer(),
                itemTransformer(),
                seriesTransformer()
        );
    }

    private BrandModelTransformer brandTransformer() {
        return new BrandModelTransformer(
                lookupStore,
                topicStore,
                idCodec(),
                clipTransformer(),
                new SystemClock()
        );
    }

    private ItemModelTransformer itemTransformer() {
        return new ItemModelTransformer(
                lookupStore,
                topicStore,
                channelResolver,
                idCodec(),
                clipTransformer(),
                new SystemClock(),
                segmentModelTransformer()
        );
    }

    private SeriesModelTransformer seriesTransformer() {
        return new SeriesModelTransformer(
                lookupStore,
                topicStore,
                idCodec(),
                clipTransformer(),
                new SystemClock()
        );
    }

    private ClipModelTransformer clipTransformer() {
        return new ClipModelTransformer(
                lookupStore,
                topicStore,
                channelResolver,
                idCodec(),
                new SystemClock(),
                segmentModelTransformer()
        );
    }

    private SegmentModelTransformer segmentModelTransformer() {
        return new SegmentModelTransformer(segmentWriter);
    }

    private NumberToShortStringCodec idCodec() {
        return SubstitutionTableNumberCodec.lowerCaseOnly();
    }
}

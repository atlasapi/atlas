package org.atlasapi.query;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.atlasapi.input.BrandModelTransformer;
import org.atlasapi.input.ClipModelTransformer;
import org.atlasapi.input.DefaultJacksonModelReader;
import org.atlasapi.input.DelegatingModelTransformer;
import org.atlasapi.input.ItemModelTransformer;
import org.atlasapi.input.SegmentModelTransformer;
import org.atlasapi.input.SeriesModelTransformer;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.segment.SegmentWriter;
import org.atlasapi.messaging.v3.KafkaMessagingModule;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.EquivalenceContentWriter;
import org.atlasapi.persistence.content.ScheduleResolver;
import org.atlasapi.persistence.content.schedule.mongo.ScheduleWriter;
import org.atlasapi.persistence.event.EventResolver;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;
import org.atlasapi.persistence.topic.TopicStore;
import org.atlasapi.query.content.ContentWriteExecutor;
import org.atlasapi.query.content.merge.ContentMerger;
import org.atlasapi.query.content.merge.EpisodeMerger;
import org.atlasapi.query.content.merge.ItemMerger;
import org.atlasapi.query.content.merge.SeriesMerger;
import org.atlasapi.query.content.merge.SongMerger;
import org.atlasapi.query.content.merge.VersionMerger;
import org.atlasapi.query.worker.ContentWriteMessage;
import org.atlasapi.query.worker.ContentWriteMessageSerialiser;
import org.atlasapi.query.worker.ContentWriteWorker;
import org.atlasapi.remotesite.channel4.pmlsd.epg.BroadcastTrimmer;
import org.atlasapi.remotesite.channel4.pmlsd.epg.ScheduleResolverBroadcastTrimmer;

import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.queue.MessageConsumerFactory;
import com.metabroadcast.common.queue.MessageSender;
import com.metabroadcast.common.queue.MessageSenderFactory;
import com.metabroadcast.common.queue.kafka.KafkaConsumer;
import com.metabroadcast.common.time.SystemClock;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.ServiceManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import static org.atlasapi.persistence.MongoContentPersistenceModule.NON_ID_SETTING_CONTENT_WRITER;

@SuppressWarnings("PublicConstructor")
@Configuration
@Import({ KafkaMessagingModule.class })
public class QueryExecutorModule {
    @Autowired @Qualifier(NON_ID_SETTING_CONTENT_WRITER) private EquivalenceContentWriter contentWriter;
    @Autowired private ScheduleWriter scheduleWriter;
    @Autowired private ScheduleResolver scheduleResolver;
    @Autowired private ContentResolver contentResolver;
    @Autowired private ChannelResolver channelResolver;
    @Autowired @Qualifier("topicStore") private TopicStore topicStore;
    @Autowired private LookupEntryStore lookupStore;
    @Autowired private EventResolver eventResolver;
    @Autowired private SegmentWriter segmentWriter;
    @Autowired private MessageConsumerFactory<KafkaConsumer> consumerFactory;
    @Autowired private MessageSenderFactory senderFactory;
    @Autowired private DatabasedMongo db;

    @Value("${messaging.system}") private String messagingSystem;
    @Value("${messaging.destination.write.content}") private String contentWriteTopic;
    @Value("${messaging.write.consumers.num}") private Integer numOfConsumers;
    @Value("${messaging.enabled.write.content}") private Boolean contentWriteWorkerEnabled;
    @Value("${processing.config}") private Boolean isProcessing;

    private ServiceManager consumerManager;

    public QueryExecutorModule(){
    }

    @PostConstruct
    public void start() throws TimeoutException {
        ImmutableList.Builder<Service> servicesBuilder = ImmutableList.builder();

        if (isProcessing && contentWriteWorkerEnabled) {
            servicesBuilder.add(contentWriteWorker());
        }

        ImmutableList<Service> services = servicesBuilder.build();

        if (!services.isEmpty()) {
            consumerManager = new ServiceManager(services);
            consumerManager.startAsync().awaitHealthy(1, TimeUnit.MINUTES);
        }
    }

    @PreDestroy
    public void stop() throws TimeoutException {
        if (consumerManager != null) {
            consumerManager.stopAsync().awaitStopped(1, TimeUnit.MINUTES);
        }
    }

    @Bean
    public ContentWriteExecutor contentWriteExecutor() {
        VersionMerger versionMerger = VersionMerger.create();
        SongMerger songMerger = SongMerger.create();
        EpisodeMerger episodeMerger = EpisodeMerger.create();
        SeriesMerger seriesMerger = SeriesMerger.create();

        ItemMerger itemMerger = ItemMerger.create(versionMerger, songMerger);

        return ContentWriteExecutor.create(
                new DefaultJacksonModelReader(),
                delegatingModelTransformer(),
                contentResolver,
                contentWriter,
                scheduleWriter,
                channelResolver,
                eventResolver,
                ContentMerger.create(itemMerger, episodeMerger, seriesMerger),
                versionMerger,
                broadcastTrimmer()
        );
    }

    @Bean
    public KafkaConsumer contentWriteWorker() {
        ContentWriteWorker worker = new ContentWriteWorker(contentWriteExecutor());
        ContentWriteMessageSerialiser messageSerialiser = new ContentWriteMessageSerialiser();

        return consumerFactory.createConsumer(
                worker,
                messageSerialiser,
                contentWriteTopic,
                "WriteContent"
        )
                .withConsumerSystem(messagingSystem)
                .withDefaultConsumers(numOfConsumers)
                .withMaxConsumers(numOfConsumers)
                .withPersistentRetryPolicy(db)
                .build();
    }

    @Bean
    public MessageSender<ContentWriteMessage> contentWriteMessageSender() {
        return senderFactory.makeMessageSender(
                contentWriteTopic, new ContentWriteMessageSerialiser()
        );
    }

    private BroadcastTrimmer broadcastTrimmer() {
        return new ScheduleResolverBroadcastTrimmer(
                null,
                scheduleResolver,
                contentResolver,
                contentWriter
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

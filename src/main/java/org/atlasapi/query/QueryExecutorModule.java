package org.atlasapi.query;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.atlasapi.input.BrandModelTransformer;
import org.atlasapi.input.ClipModelTransformer;
import org.atlasapi.input.DefaultGsonModelReader;
import org.atlasapi.input.DelegatingModelTransformer;
import org.atlasapi.input.ItemModelTransformer;
import org.atlasapi.input.SegmentModelTransformer;
import org.atlasapi.input.SeriesModelTransformer;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.segment.SegmentWriter;
import org.atlasapi.messaging.v3.KafkaMessagingModule;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.schedule.mongo.ScheduleWriter;
import org.atlasapi.persistence.event.EventResolver;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;
import org.atlasapi.persistence.topic.TopicStore;
import org.atlasapi.query.content.ContentWriteExecutor;
import org.atlasapi.query.worker.ContentWriteMessage;
import org.atlasapi.query.worker.ContentWriteMessageSerialiser;
import org.atlasapi.query.worker.ContentWriteWorker;

import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
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

@Configuration
@Import({ KafkaMessagingModule.class })
public class QueryExecutorModule {

    private @Autowired @Qualifier(NON_ID_SETTING_CONTENT_WRITER) ContentWriter contentWriter;
    private @Autowired ScheduleWriter scheduleWriter;
    private @Autowired ContentResolver contentResolver;
    private @Autowired ChannelResolver channelResolver;
    private @Autowired @Qualifier("topicStore") TopicStore topicStore;
    private @Autowired LookupEntryStore lookupStore;
    private @Autowired EventResolver eventResolver;
    private @Autowired SegmentWriter segmentWriter;
    private @Autowired MessageConsumerFactory<KafkaConsumer> consumerFactory;
    private @Autowired MessageSenderFactory senderFactory;

    private @Value("${messaging.system}") String messagingSystem;
    private @Value("${messaging.destination.write.content}") String contentWriteTopic;
    private @Value("${messaging.write.consumers.num}") Integer numOfConsumers;
    private @Value("${messaging.enabled.write.content}") Boolean contentWriteWorkerEnabled;
    private @Value("${processing.config}") Boolean isProcessing;

    private ServiceManager consumerManager;

    @PostConstruct
    public void start() throws TimeoutException {
        ImmutableList.Builder<Service> services = ImmutableList.builder();

        if (isProcessing && contentWriteWorkerEnabled) {
            services.add(contentWriteWorker());
        }

        consumerManager = new ServiceManager(services.build());
        consumerManager.startAsync().awaitHealthy(1, TimeUnit.MINUTES);
    }

    @PreDestroy
    public void stop() throws TimeoutException {
        consumerManager.stopAsync().awaitStopped(1, TimeUnit.MINUTES);
    }

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
                .build();
    }

    @Bean
    public MessageSender<ContentWriteMessage> contentWriteMessageSender() {
        return senderFactory.makeMessageSender(
                contentWriteTopic, new ContentWriteMessageSerialiser()
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

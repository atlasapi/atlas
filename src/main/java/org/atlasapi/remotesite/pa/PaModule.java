package org.atlasapi.remotesite.pa;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.scheduling.RepetitionRule;
import com.metabroadcast.common.scheduling.RepetitionRules;
import com.metabroadcast.common.scheduling.SimpleScheduler;
import com.metabroadcast.common.security.UsernameAndPassword;
import com.metabroadcast.common.time.DayOfWeek;
import com.mongodb.DBCollection;
import org.atlasapi.equiv.EquivalenceBreaker;
import org.atlasapi.equiv.PaAliasBackPopulatorTask;
import org.atlasapi.feeds.upload.persistence.FileUploadResultStore;
import org.atlasapi.feeds.upload.persistence.MongoFileUploadResultStore;
import org.atlasapi.media.channel.ChannelGroupResolver;
import org.atlasapi.media.channel.ChannelGroupWriter;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.channel.ChannelWriter;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.ContentCategory;
import org.atlasapi.persistence.content.ContentGroupResolver;
import org.atlasapi.persistence.content.ContentGroupWriter;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.PeopleResolver;
import org.atlasapi.persistence.content.ScheduleResolver;
import org.atlasapi.persistence.content.listing.ContentLister;
import org.atlasapi.persistence.content.mongo.MongoContentTables;
import org.atlasapi.persistence.content.people.ItemsPeopleWriter;
import org.atlasapi.persistence.content.people.PersonWriter;
import org.atlasapi.persistence.content.schedule.mongo.ScheduleWriter;
import org.atlasapi.persistence.content.tasks.MongoScheduleTaskProgressStore;
import org.atlasapi.persistence.ids.MongoSequentialIdGenerator;
import org.atlasapi.persistence.logging.AdapterLog;
import org.atlasapi.persistence.logging.AdapterLogEntry;
import org.atlasapi.persistence.logging.AdapterLogEntry.Severity;
import org.atlasapi.persistence.lookup.LookupWriter;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;
import org.atlasapi.persistence.topic.TopicStore;
import org.atlasapi.remotesite.channel4.pmlsd.epg.BroadcastTrimmer;
import org.atlasapi.remotesite.channel4.pmlsd.epg.ScheduleResolverBroadcastTrimmer;
import org.atlasapi.remotesite.pa.archives.PaArchivesUpdater;
import org.atlasapi.remotesite.pa.archives.PaCompleteArchivesUpdater;
import org.atlasapi.remotesite.pa.archives.PaProgDataUpdatesProcessor;
import org.atlasapi.remotesite.pa.archives.PaRecentArchiveUpdater;
import org.atlasapi.remotesite.pa.archives.PaUpdatesProcessor;
import org.atlasapi.remotesite.pa.channels.PaChannelDataHandler;
import org.atlasapi.remotesite.pa.channels.PaChannelGroupsIngester;
import org.atlasapi.remotesite.pa.channels.PaChannelsIngester;
import org.atlasapi.remotesite.pa.channels.PaChannelsUpdater;
import org.atlasapi.remotesite.pa.data.DefaultPaProgrammeDataStore;
import org.atlasapi.remotesite.pa.data.PaProgrammeDataStore;
import org.atlasapi.remotesite.pa.deletes.ExistingItemUnPublisher;
import org.atlasapi.remotesite.pa.deletes.PaContentDeactivator;
import org.atlasapi.remotesite.pa.deletes.PaContentDeactivatorTask;
import org.atlasapi.remotesite.pa.features.ContentGroupDetails;
import org.atlasapi.remotesite.pa.features.PaFeaturesConfiguration;
import org.atlasapi.remotesite.pa.features.PaFeaturesContentGroupProcessor;
import org.atlasapi.remotesite.pa.features.PaFeaturesProcessor;
import org.atlasapi.remotesite.pa.features.PaFeaturesUpdater;
import org.atlasapi.remotesite.pa.people.PaCompletePeopleUpdater;
import org.atlasapi.remotesite.pa.people.PaDailyPeopleUpdater;
import org.atlasapi.remotesite.pa.persistence.MongoPaScheduleVersionStore;
import org.atlasapi.remotesite.pa.persistence.PaScheduleVersionStore;
import org.atlasapi.remotesite.rt.RtFilmModule;
import org.atlasapi.s3.DefaultS3Client;
import org.atlasapi.s3.S3Client;
import org.joda.time.Duration;
import org.joda.time.LocalTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import javax.annotation.PostConstruct;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.metabroadcast.common.scheduling.RepetitionRules.NEVER;
import static com.metabroadcast.common.scheduling.RepetitionRules.daily;
import static com.metabroadcast.common.scheduling.RepetitionRules.every;
import static com.metabroadcast.common.scheduling.RepetitionRules.weekly;
import static org.atlasapi.persistence.MongoContentPersistenceModule.EXPLICIT_LOOKUP_WRITER;

@SuppressWarnings("PublicConstructor")
@Configuration
@Import(RtFilmModule.class)
public class PaModule {

    private final static RepetitionRule PEOPLE_COMPLETE_INGEST = NEVER;
    private final static RepetitionRule PEOPLE_INGEST = daily(LocalTime.MIDNIGHT);
    private final static RepetitionRule CHANNELS_INGEST = every(Duration.standardHours(2));
    private final static RepetitionRule FEATURES_INGEST = daily(LocalTime.MIDNIGHT);
    private final static RepetitionRule RECENT_FILE_INGEST = every(Duration.standardMinutes(10))
            .withOffset(Duration.standardMinutes(15));
    private final static RepetitionRule RECENT_ALL_FILE_INGEST = NEVER;
    private final static RepetitionRule RECENT_FILE_DOWNLOAD = every(Duration.standardMinutes(10));
    private final static RepetitionRule COMPLETE_INGEST = NEVER;
    private static final RepetitionRules.Weekly CONTENT_DEACTIVATOR = weekly(
            DayOfWeek.MONDAY, LocalTime.MIDNIGHT
    );

    private static final String TOPICS_COLLECTION = "topics";

    private @Autowired SimpleScheduler scheduler;
    private @Autowired ContentWriter contentWriter;
    private @Autowired @Qualifier("contentResolver") ContentResolver contentResolver;
    private @Autowired ContentGroupWriter contentGroupWriter;
    private @Autowired ContentGroupResolver contentGroupResolver;
    private @Autowired PeopleResolver personResolver;
    private @Autowired PersonWriter personWriter;
    private @Autowired ChannelGroupWriter channelGroupWriter;
    private @Autowired ChannelGroupResolver channelGroupResolver;
    private @Autowired AdapterLog log;
    private @Autowired ScheduleResolver scheduleResolver;
    private @Autowired ItemsPeopleWriter peopleWriter;
    private @Autowired ScheduleWriter scheduleWriter;
    private @Autowired ChannelResolver channelResolver;
    private @Autowired ChannelWriter channelWriter;
    private @Autowired DatabasedMongo mongo;
    private @Autowired @Qualifier("topicStore") TopicStore topicStore;
    private @Autowired ContentLister contentLister;
    private @Autowired LookupEntryStore lookupEntryStore;
    private @Autowired LookupWriter lookupWriter;
    @Autowired @Qualifier(EXPLICIT_LOOKUP_WRITER) private LookupWriter explicitLookupWriter;

    // to ensure the complete and daily people ingest jobs are not run simultaneously 
    private final Lock peopleLock = new ReentrantLock();

    private @Value("${pa.ftp.username}") String ftpUsername;
    private @Value("${pa.ftp.password}") String ftpPassword;
    private @Value("${pa.ftp.host}") String ftpHost;
    private @Value("${pa.ftp.path}") String ftpPath;
    private @Value("${pa.filesPath}") String localFilesPath;
    private @Value("${s3.access}") String s3access;
    private @Value("${s3.secret}") String s3secret;
    private @Value("${pa.s3.bucket}") String s3bucket;
    private @Value("${pa.people.enabled}") boolean peopleEnabled;
    private @Value("${pa.content.updater.threads}") int contentUpdaterThreadCount;

    @PostConstruct
    public void startBackgroundTasks() {
        if (peopleEnabled) {
            scheduler.schedule(
                    paCompletePeopleUpdater().withName("PA Complete People Updater"),
                    PEOPLE_COMPLETE_INGEST
            );
            scheduler.schedule(
                    paDailyPeopleUpdater().withName("PA People Updater"),
                    PEOPLE_INGEST
            );
        }

        scheduler.schedule(
                paChannelsUpdater().withName("PA Channels Updater"),
                CHANNELS_INGEST
        );
        scheduler.schedule(
                paFeaturesUpdater().withName("PA Features Updater"),
                FEATURES_INGEST
        );
        scheduler.schedule(
                paFileUpdater().withName("PA File Updater"),
                RECENT_FILE_DOWNLOAD
        );
        scheduler.schedule(
                paCompleteUpdater().withName("PA Complete Updater"),
                COMPLETE_INGEST
        );
        scheduler.schedule(
                paRecentUnprocessedUpdater().withName("PA Recent Unprocessed Updater"),
                RECENT_FILE_INGEST
        );
        scheduler.schedule(
                paRecentAllUpdater().withName(
                        "PA Recent All Updater (Will re-ingest even successfully processed)"
                ),
                RECENT_ALL_FILE_INGEST
        );
        scheduler.schedule(
                paRecentArchivesUpdater().withName("PA Recent Archives Updater"),
                RECENT_FILE_INGEST
        );
        scheduler.schedule(
                paCompleteArchivesUpdater().withName("PA Complete Archives Updater"),
                COMPLETE_INGEST
        );
        scheduler.schedule(
                PaContentDeactivatorTask().withName("PA Content Deactivator"),
                CONTENT_DEACTIVATOR
        );

        log.record(
                new AdapterLogEntry(Severity.INFO)
                        .withDescription("PA update scheduled task installed")
                        .withSource(PaCompleteUpdater.class)
        );
    }

    @Bean
    PaTagMap paTagMap() {
        return new PaTagMap(topicStore, new MongoSequentialIdGenerator(mongo, TOPICS_COLLECTION));
    }

    @Bean
    PaChannelsUpdater paChannelsUpdater() {
        return new PaChannelsUpdater(
                paProgrammeDataStore(),
                channelDataHandler(),
                channelWriterLock()
        );
    }

    @Bean
    PaChannelDataHandler channelDataHandler() {
        return new PaChannelDataHandler(
                new PaChannelsIngester(),
                new PaChannelGroupsIngester(),
                channelResolver,
                channelWriter,
                channelGroupResolver,
                channelGroupWriter
        );
    }

    @Bean
    PaFeaturesUpdater paFeaturesUpdater() {
        return new PaFeaturesUpdater(
                paProgrammeDataStore(),
                fileUploadResultStore(),
                new PaFeaturesProcessor(contentResolver),
                new PaFeaturesContentGroupProcessor(
                        contentGroupResolver,
                        contentGroupWriter,
                        featuresConfiguration()
                )
        );
    }

    @Bean
    PaFeaturesConfiguration featuresConfiguration() {
        return new PaFeaturesConfiguration(
                ImmutableMap.<String, ContentGroupDetails>builder()
                    .put(
                            "General",
                            new ContentGroupDetails(
                                    Publisher.PA_FEATURES,
                                    "http://pressassocation.com/features/tvpicks"
                            )
                    )
                    .put(
                            "Ireland",
                            new ContentGroupDetails(
                                    Publisher.PA_FEATURES_IRELAND,
                                    "http://pressassocation.com/features/ireland"
                            )
                    )
                    .put(
                            "Soap-Entertainment",
                            new ContentGroupDetails(
                                    Publisher.PA_FEATURES_SOAP_ENTERTAINMENT,
                                    "http://pressassocation.com/features/soap-entertainment"
                            )
                    )
                    .build()
        );
    }

    @Bean
    PaFtpFileUpdater ftpFileUpdater() {
        return new PaFtpFileUpdater(
                ftpHost,
                new UsernameAndPassword(ftpUsername, ftpPassword),
                ftpPath,
                paProgrammeDataStore()
        );
    }

    @Bean
    PaProgrammeDataStore paProgrammeDataStore() {
        S3Client s3client = new DefaultS3Client(s3access, s3secret, s3bucket);

        return new DefaultPaProgrammeDataStore(localFilesPath, s3client);
    }

    @Bean
    PaProgDataProcessor paProgrammeProcessor() {
        return PaProgrammeProcessor.create(
                contentBuffer(),
                log,
                paTagMap(),
                existingItemUnPublisher()
        );
    }

    @Bean
    PaCompleteUpdater paCompleteUpdater() {
        PaEmptyScheduleProcessor processor = new PaEmptyScheduleProcessor(
                paProgrammeProcessor(),
                scheduleResolver
        );

        PaChannelProcessor channelProcessor = PaChannelProcessor.builder()
                .withProcessor(processor)
                .withTrimmer(broadcastTrimmer())
                .withScheduleWriter(scheduleWriter)
                .withScheduleVersionStore(paScheduleVersionStore())
                .withContentBuffer(contentBuffer())
                .build();

        ExecutorService executor = Executors.newFixedThreadPool(
                contentUpdaterThreadCount,
                new ThreadFactoryBuilder().setNameFormat("pa-complete-updater-%s").build()
        );

        return new PaCompleteUpdater(
                executor,
                channelProcessor,
                paProgrammeDataStore(),
                channelResolver
        );
    }

    @Bean
    PaRecentUpdater paRecentUnprocessedUpdater() {
        PaChannelProcessor channelProcessor = PaChannelProcessor.builder()
                .withProcessor(paProgrammeProcessor())
                .withTrimmer(broadcastTrimmer())
                .withScheduleWriter(scheduleWriter)
                .withScheduleVersionStore(paScheduleVersionStore())
                .withContentBuffer(contentBuffer())
                .build();

        ExecutorService executor = Executors.newFixedThreadPool(
                contentUpdaterThreadCount,
                new ThreadFactoryBuilder().setNameFormat("pa-recent-unprocessed-updater-%s").build()
        );

        return new PaRecentUpdater(
                executor,
                channelProcessor,
                paProgrammeDataStore(),
                channelResolver,
                fileUploadResultStore(),
                paScheduleVersionStore(),
                PaBaseProgrammeUpdater.Mode.NORMAL
        );
    }

    @Bean
    PaRecentUpdater paRecentAllUpdater() {
        PaChannelProcessor channelProcessor = PaChannelProcessor.builder()
                .withProcessor(paProgrammeProcessor())
                .withTrimmer(broadcastTrimmer())
                .withScheduleWriter(scheduleWriter)
                .withScheduleVersionStore(paScheduleVersionStore())
                .withContentBuffer(contentBuffer())
                .build();

        ExecutorService executor = Executors.newFixedThreadPool(
                contentUpdaterThreadCount,
                new ThreadFactoryBuilder().setNameFormat("pa-recent-all-updater-%s").build()
        );

        return new PaRecentUpdater(
                executor,
                channelProcessor,
                paProgrammeDataStore(),
                channelResolver,
                fileUploadResultStore(),
                paScheduleVersionStore(),
                PaBaseProgrammeUpdater.Mode.BOOTSTRAP
        );
    }

    @Bean
    PaArchivesUpdater paRecentArchivesUpdater() {
        PaProgDataUpdatesProcessor paProgDataUpdatesProcessor = PaProgrammeProcessor.create(
                contentBuffer(),
                log,
                paTagMap(),
                existingItemUnPublisher()
        );

        PaUpdatesProcessor updatesProcessor = PaUpdatesProcessor.create(
                paProgDataUpdatesProcessor, contentWriter
        );

        return new PaRecentArchiveUpdater(
                paProgrammeDataStore(), fileUploadResultStore(), updatesProcessor
        );
    }

    @Bean
    PaArchivesUpdater paCompleteArchivesUpdater() {
        PaProgDataUpdatesProcessor paProgDataUpdatesProcessor = PaProgrammeProcessor.create(
                contentBuffer(),
                log,
                paTagMap(),
                existingItemUnPublisher()
        );

        PaUpdatesProcessor updatesProcessor = PaUpdatesProcessor.create(
                paProgDataUpdatesProcessor, contentWriter
        );

        return new PaCompleteArchivesUpdater(
                paProgrammeDataStore(), fileUploadResultStore(), updatesProcessor
        );
    }

    @Bean @Qualifier("PaContentBuffer")
    ContentBuffer contentBuffer() {
        return ContentBuffer.create(contentResolver, contentWriter, peopleWriter);
    }

    @Bean
    BroadcastTrimmer broadcastTrimmer() {
        return new ScheduleResolverBroadcastTrimmer(
                Publisher.PA,
                scheduleResolver,
                contentResolver,
                contentWriter
        );
    }

    @Bean
    PaFileUpdater paFileUpdater() {
        return new PaFileUpdater(ftpFileUpdater());
    }

    @Bean
    public PaSingleDateUpdatingController paUpdateController() {
        PaChannelProcessor channelProcessor = PaChannelProcessor.builder()
                .withProcessor(paProgrammeProcessor())
                .withTrimmer(broadcastTrimmer())
                .withScheduleWriter(scheduleWriter)
                .withScheduleVersionStore(paScheduleVersionStore())
                .withContentBuffer(contentBuffer())
                .build();

        return new PaSingleDateUpdatingController(
                channelProcessor,
                channelResolver,
                paProgrammeDataStore(),
                paScheduleVersionStore()
        );
    }

    @Bean
    public PaScheduleVersionStore paScheduleVersionStore() {
        return new MongoPaScheduleVersionStore(mongo);
    }

    @Bean
    public ReentrantLock channelWriterLock() {
        return new ReentrantLock();
    }

    @Bean
    public PaContentDeactivatorTask PaContentDeactivatorTask() {
        return new PaContentDeactivatorTask(
                paContentDeactivator(),
                paProgrammeDataStore(),
                false
        );
    }

    @Bean @Qualifier("dryRunPaContentDeactivatorTask")
    public PaContentDeactivatorTask dryRunPaContentDeactivatorTask() {
        return new PaContentDeactivatorTask(
                paContentDeactivator(),
                paProgrammeDataStore(),
                true
        );
    }

    @Bean
    public PaContentDeactivator paContentDeactivator() {
        DBCollection childrenDb = new MongoContentTables(mongo)
                .collectionFor(ContentCategory.CHILD_ITEM);
        return new PaContentDeactivator(
                contentLister,
                contentWriter,
                new MongoScheduleTaskProgressStore(mongo),
                childrenDb
        );
    }

    @Bean
    public PaAliasBackPopulatorTask paAliasBackPopulationTask() {
        PaAliasBackPopulator aliasBackPopulator = new PaAliasBackPopulator(
                contentLister,
                contentWriter,
                new MongoScheduleTaskProgressStore(mongo)
        );
        return new PaAliasBackPopulatorTask(aliasBackPopulator, false);
    }

    private PaCompletePeopleUpdater paCompletePeopleUpdater() {
        return new PaCompletePeopleUpdater(
                paProgrammeDataStore(),
                personResolver,
                personWriter,
                peopleLock
        );
    }

    private PaDailyPeopleUpdater paDailyPeopleUpdater() {
        return new PaDailyPeopleUpdater(
                paProgrammeDataStore(),
                personResolver,
                personWriter,
                fileUploadResultStore(),
                peopleLock
        );
    }

    private FileUploadResultStore fileUploadResultStore() {
        return new MongoFileUploadResultStore(mongo);
    }

    private ExistingItemUnPublisher existingItemUnPublisher() {
        return ExistingItemUnPublisher.create(
                contentResolver,
                contentWriter,
                lookupEntryStore,
                EquivalenceBreaker.create(
                        contentResolver,
                        lookupEntryStore,
                        lookupWriter,
                        explicitLookupWriter
                ),
                true
        );
    }
}

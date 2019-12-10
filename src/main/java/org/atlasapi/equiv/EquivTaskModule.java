package org.atlasapi.equiv;

import com.google.api.client.util.Sets;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service.Listener;
import com.google.common.util.concurrent.Service.State;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.queue.kafka.KafkaConsumer;
import com.metabroadcast.common.scheduling.RepetitionRule;
import com.metabroadcast.common.scheduling.RepetitionRules;
import com.metabroadcast.common.scheduling.ScheduledTask;
import com.metabroadcast.common.scheduling.SimpleScheduler;
import org.atlasapi.equiv.results.persistence.RecentEquivalenceResultStore;
import org.atlasapi.equiv.results.probe.EquivalenceProbeStore;
import org.atlasapi.equiv.results.probe.EquivalenceResultProbeController;
import org.atlasapi.equiv.results.probe.MongoEquivalenceProbeStore;
import org.atlasapi.equiv.results.www.EquivGraphController;
import org.atlasapi.equiv.results.www.EquivalenceResultController;
import org.atlasapi.equiv.results.www.RecentResultController;
import org.atlasapi.equiv.update.EquivalenceUpdater;
import org.atlasapi.equiv.update.RecoveringEquivalenceUpdater;
import org.atlasapi.equiv.update.tasks.ContentEquivalenceUpdateTask;
import org.atlasapi.equiv.update.tasks.DeltaContentEquivalenceUpdateTask;
import org.atlasapi.equiv.update.tasks.MongoScheduleTaskProgressStore;
import org.atlasapi.equiv.update.tasks.ScheduleEquivalenceUpdateTask;
import org.atlasapi.equiv.update.tasks.ScheduleEquivalenceUpdateTask.Builder;
import org.atlasapi.equiv.update.www.ContentEquivalenceUpdateController;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.messaging.v3.EntityUpdatedMessage;
import org.atlasapi.messaging.v3.JacksonMessageSerializer;
import org.atlasapi.messaging.v3.KafkaMessagingModule;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ScheduleResolver;
import org.atlasapi.persistence.content.listing.SelectedContentLister;
import org.atlasapi.persistence.content.mongo.LastUpdatedContentFinder;
import org.atlasapi.persistence.lookup.LookupWriter;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;
import org.atlasapi.remotesite.bbc.ion.BbcIonServices;
import org.atlasapi.remotesite.bbc.nitro.channels.NitroChannelMap;
import org.atlasapi.remotesite.channel4.pmlsd.C4AtomApi;
import org.atlasapi.remotesite.five.FiveChannelMap;
import org.atlasapi.remotesite.itv.whatson.ItvWhatsonChannelMap;
import org.atlasapi.remotesite.redux.ReduxServices;
import org.atlasapi.remotesite.youview.YouViewChannelResolver;
import org.atlasapi.remotesite.youview.YouViewCoreModule;
import org.atlasapi.util.AlwaysBlockingQueue;
import org.joda.time.Duration;
import org.joda.time.LocalTime;
import org.joda.time.Period;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Lazy;

import javax.annotation.PostConstruct;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.atlasapi.equiv.update.tasks.ContentEquivalenceUpdateTask.SAVE_EVERY_BLOCK_SIZE;
import static org.atlasapi.media.entity.Publisher.AMAZON_UNBOX;
import static org.atlasapi.media.entity.Publisher.AMC_EBS;
import static org.atlasapi.media.entity.Publisher.BARB_CENSUS;
import static org.atlasapi.media.entity.Publisher.BARB_MASTER;
import static org.atlasapi.media.entity.Publisher.BARB_NLE;
import static org.atlasapi.media.entity.Publisher.BARB_TRANSMISSIONS;
import static org.atlasapi.media.entity.Publisher.BARB_X_MASTER;
import static org.atlasapi.media.entity.Publisher.BBC;
import static org.atlasapi.media.entity.Publisher.BBC_MUSIC;
import static org.atlasapi.media.entity.Publisher.BBC_NITRO;
import static org.atlasapi.media.entity.Publisher.BBC_REDUX;
import static org.atlasapi.media.entity.Publisher.BETTY;
import static org.atlasapi.media.entity.Publisher.BT_SPORT_EBS;
import static org.atlasapi.media.entity.Publisher.BT_TVE_VOD;
import static org.atlasapi.media.entity.Publisher.BT_VOD;
import static org.atlasapi.media.entity.Publisher.C4_PMLSD;
import static org.atlasapi.media.entity.Publisher.C4_PRESS;
import static org.atlasapi.media.entity.Publisher.C5_DATA_SUBMISSION;
import static org.atlasapi.media.entity.Publisher.EBMS_VF_UK;
import static org.atlasapi.media.entity.Publisher.FIVE;
import static org.atlasapi.media.entity.Publisher.IMDB;
import static org.atlasapi.media.entity.Publisher.IMDB_API;
import static org.atlasapi.media.entity.Publisher.ITUNES;
import static org.atlasapi.media.entity.Publisher.ITV;
import static org.atlasapi.media.entity.Publisher.ITV_CPS;
import static org.atlasapi.media.entity.Publisher.ITV_INTERLINKING;
import static org.atlasapi.media.entity.Publisher.LAYER3_TXLOGS;
import static org.atlasapi.media.entity.Publisher.LOVEFILM;
import static org.atlasapi.media.entity.Publisher.NETFLIX;
import static org.atlasapi.media.entity.Publisher.PA;
import static org.atlasapi.media.entity.Publisher.RADIO_TIMES;
import static org.atlasapi.media.entity.Publisher.REDBEE_MEDIA;
import static org.atlasapi.media.entity.Publisher.ROVI_EN;
import static org.atlasapi.media.entity.Publisher.RTE;
import static org.atlasapi.media.entity.Publisher.TALK_TALK;
import static org.atlasapi.media.entity.Publisher.UKTV;
import static org.atlasapi.media.entity.Publisher.VF_AE;
import static org.atlasapi.media.entity.Publisher.VF_BBC;
import static org.atlasapi.media.entity.Publisher.VF_C5;
import static org.atlasapi.media.entity.Publisher.VF_ITV;
import static org.atlasapi.media.entity.Publisher.VF_VIACOM;
import static org.atlasapi.media.entity.Publisher.VF_VUBIQUITY;
import static org.atlasapi.media.entity.Publisher.WIKIPEDIA;
import static org.atlasapi.media.entity.Publisher.YOUVIEW;
import static org.atlasapi.media.entity.Publisher.YOUVIEW_BT;
import static org.atlasapi.media.entity.Publisher.YOUVIEW_BT_STAGE;
import static org.atlasapi.media.entity.Publisher.YOUVIEW_SCOTLAND_RADIO;
import static org.atlasapi.media.entity.Publisher.YOUVIEW_SCOTLAND_RADIO_STAGE;
import static org.atlasapi.media.entity.Publisher.YOUVIEW_STAGE;
import static org.atlasapi.persistence.MongoContentPersistenceModule.EXPLICIT_LOOKUP_WRITER;

@Configuration
@Import({ EquivModule.class, KafkaMessagingModule.class, YouViewCoreModule.class })
public class EquivTaskModule {
    private final Logger log = LoggerFactory.getLogger(getClass());

    private static final int EQUIV_THREADS_PER_JOB = 5;

    private static final Set<String> ignored =
            ImmutableSet.of("http://www.bbc.co.uk/programmes/b006mgyl");
    private static final RepetitionRule RT_EQUIVALENCE_REPETITION =
            RepetitionRules.every(Duration.standardDays(2));
    private static final RepetitionRule TALKTALK_EQUIVALENCE_REPETITION =
            RepetitionRules.NEVER;
    private static final RepetitionRule YOUVIEW_SCHEDULE_EQUIVALENCE_REPETITION =
            RepetitionRules.every(Duration.standardHours(4));
    private static final RepetitionRule YOUVIEW_STAGE_SCHEDULE_EQUIVALENCE_REPETITION =
            RepetitionRules.daily(new LocalTime(12, 0));
    private static final RepetitionRule BBC_SCHEDULE_EQUIVALENCE_REPETITION =
            RepetitionRules.daily(new LocalTime(12, 0));
    private static final RepetitionRule ITV_SCHEDULE_EQUIVALENCE_REPETITION =
            RepetitionRules.NEVER;
    private static final RepetitionRule ITV_EQUIVALENCE_REPETITION =
            RepetitionRules.NEVER;
    private static final RepetitionRule C4_SCHEDULE_EQUIVALENCE_REPETITION =
            RepetitionRules.daily(new LocalTime(15, 0));
    private static final RepetitionRule FIVE_SCHEDULE_EQUIVALENCE_REPETITION =
            RepetitionRules.NEVER;
    private static final RepetitionRule REDUX_SCHEDULE_EQUIVALENCE_REPETITION =
            RepetitionRules.NEVER;
    private static final RepetitionRule ROVI_EN_EQUIVALENCE_REPETITION =
            RepetitionRules.NEVER;
    private static final RepetitionRule RTE_EQUIVALENCE_REPETITION =
            RepetitionRules.every(Duration.standardDays(2));
    private static final RepetitionRule BT_VOD_EQUIVALENCE_REPETITION =
            RepetitionRules.NEVER;
    private static final RepetitionRule AMAZON_EQUIVALENCE_REPETITION = RepetitionRules.NEVER;
    private static final RepetitionRule AMAZON_EQUIVALENCE_DELTA_48H_REPETITION =
            RepetitionRules.daily(new LocalTime(3, 0)); //This is timed with the ingest. Do not move.
    private static final RepetitionRule AMAZON_EQUIVALENCE_DELTA_240H_REPETITION = RepetitionRules.NEVER; //To allow a manual run if things went wrong for a few days
    private static final RepetitionRule UKTV_EQUIVALENCE_REPETITION =
            RepetitionRules.daily(new LocalTime(20, 0));
    private static final RepetitionRule WIKIPEDIA_EQUIVALENCE_REPETITION =
            RepetitionRules.NEVER;
    private static final RepetitionRule BBC_MUSIC_EQUIVALENCE_REPETITION =
            RepetitionRules.NEVER;
    private static final RepetitionRule TXLOGS_EQUIVALENCE_DELTA_REPETITION =
            RepetitionRules.daily(new LocalTime(9, 0));
    private static final RepetitionRule LAYER3_TXLOGS_EQUIVALENCE_DELTA_REPETITION =
            RepetitionRules.daily(new LocalTime(15, 0));
    private static final RepetitionRule XCDMF_EQUIVALENCE_DELTA_REPETITION =
            RepetitionRules.daily(new LocalTime(1, 30));
    private static final RepetitionRule CDMF_EQUIVALENCE_DELTA_REPETITION =
            RepetitionRules.daily(new LocalTime(9, 0));
    private static final RepetitionRule IMDB_API_EQUIVALENCE_REPETITION =
            RepetitionRules.NEVER;
    private static final RepetitionRule C5_DATA_SUBMISSION_EQUIVALENCE_REPETITION =
            RepetitionRules.daily(new LocalTime(3, 0));
    private static final RepetitionRule BARB_CENSUS_EQUIVALENCE_DELTA_REPETITION =
            RepetitionRules.daily(new LocalTime(7, 0));
    private static final RepetitionRule BARB_NLE_EQUIVALENCE_DELTA_REPETITION =
            RepetitionRules.daily(new LocalTime(7, 0));
    private static final RepetitionRule IMDB_EQUIVALENCE_REPETITION =
            RepetitionRules.NEVER;
    private static final RepetitionRule IMDB_EQUIVALENCE_DELTA_REPETITION =
            RepetitionRules.daily(new LocalTime(23, 0));
    private static final RepetitionRule ITUNES_EQUIVALENCE_REPETITION = RepetitionRules.NEVER;
    private static final RepetitionRule VF_BBC_EQUIVALENCE_REPETITION = RepetitionRules.NEVER;
    private static final RepetitionRule VF_C5_EQUIVALENCE_REPETITION = RepetitionRules.NEVER;
    private static final RepetitionRule VF_ITV_EQUIVALENCE_REPETITION = RepetitionRules.NEVER;
    private static final RepetitionRule VF_AE_EQUIVALENCE_REPETITION = RepetitionRules.NEVER;
    private static final RepetitionRule VF_VIACOM_EQUIVALENCE_REPETITION = RepetitionRules.NEVER;
    private static final RepetitionRule VF_VUBIQUITY_EQUIVALENCE_REPETITION = RepetitionRules.NEVER;
    private static final RepetitionRule EBMS_VF_EQUIVALENCE_REPETITION = RepetitionRules.NEVER;
    private static final RepetitionRule REDBEE_MEDIA_EQUIVALENCE_REPETITION = RepetitionRules.NEVER;
    private static final RepetitionRule AMC_EBS_EQUIVALENCE_REPETITION =
            RepetitionRules.NEVER;
    private static final RepetitionRule EBS_SPORTS_EQUIVALENCE_REPETITION =
            RepetitionRules.daily(new LocalTime(4, 0));
    private static final RepetitionRule C4_PRESS_EQUIVALENCE_REPETITION =
            RepetitionRules.NEVER;
    private static final RepetitionRule NITRO_EQUIVALENCE_REPETITION =
            RepetitionRules.daily(new LocalTime(5, 0));

    @Value("${equiv.updater.enabled}") private String updaterEnabled;
    @Value("${equiv.updater.youviewschedule.enabled}")private String youViewScheduleUpdaterEnabled;
    @Value("${equiv.stream-updater.enabled}") private Boolean streamedChangesUpdateEquiv;
    @Value("${equiv.stream-updater.consumers.default}")
    private Integer defaultStreamedEquivUpdateConsumers;
    @Value("${equiv.stream-updater.consumers.max}") private Integer maxStreamedEquivUpdateConsumers;
    @Value("${messaging.destination.content.changes}") private String contentChanges;

    @Autowired private SelectedContentLister contentLister;
    @Autowired private SimpleScheduler taskScheduler;
    @Autowired private ContentResolver contentResolver;
    @Autowired private LastUpdatedContentFinder contentFinder;
    @Autowired private DatabasedMongo db;
    @Autowired private LookupEntryStore lookupStore;
    @Autowired private ScheduleResolver scheduleResolver;
    @Autowired private ChannelResolver channelResolver;
    @Autowired private LookupWriter lookupWriter;
    @Autowired @Qualifier(EXPLICIT_LOOKUP_WRITER) private LookupWriter explicitLookupWriter;
    @Autowired private YouViewChannelResolver youviewChannelResolver;

    @Autowired @Qualifier("contentUpdater") private EquivalenceUpdater<Content> equivUpdater;
    @Autowired private RecentEquivalenceResultStore equivalenceResultStore;

    @Autowired private KafkaMessagingModule messaging;

    private final int NUM_OF_THREADS_FOR_STARTUP_JOBS = 3;

    @PostConstruct
    public void scheduleUpdater() {
        ExecutorService executorService = Executors.newFixedThreadPool(
                NUM_OF_THREADS_FOR_STARTUP_JOBS);

        Set<ScheduledTask> jobsAtStartup = Sets.newHashSet();

        if (Boolean.parseBoolean(youViewScheduleUpdaterEnabled)) {
            addYouViewScheduleEquivalenceJobs(jobsAtStartup);
        }

        if (Boolean.parseBoolean(updaterEnabled)) {
            addEquivalenceJobs(jobsAtStartup);
        }

        for (ScheduledTask scheduledTask : jobsAtStartup) {
            executorService.submit(scheduledTask);
        }
    }

    private void addEquivalenceJobs(Set<ScheduledTask> jobsAtStartup) {
        scheduleEquivalenceJob(
                publisherUpdateTask(ITV).withName("ITV Equivalence Updater"),
                ITUNES_EQUIVALENCE_REPETITION,
                jobsAtStartup
        );
        scheduleEquivalenceJob(
                publisherUpdateTask(ITV_INTERLINKING)
                        .withName("ITV Interlinking Equivalence Updater"),
                ITV_EQUIVALENCE_REPETITION,
                jobsAtStartup
        );
        scheduleEquivalenceJob(
                publisherUpdateTask(FIVE).withName("Five Equivalence Updater"),
                FIVE_SCHEDULE_EQUIVALENCE_REPETITION,
                jobsAtStartup
        );
        scheduleEquivalenceJob(
                publisherUpdateTask(ITUNES).withName("Itunes Equivalence Updater"),
                ITUNES_EQUIVALENCE_REPETITION,
                jobsAtStartup
        );
        scheduleEquivalenceJob(
                publisherUpdateTask(RADIO_TIMES).withName("RT Equivalence Updater"),
                RT_EQUIVALENCE_REPETITION,
                jobsAtStartup
        );
        scheduleEquivalenceJob(
                publisherUpdateTask(TALK_TALK).withName("TalkTalk Equivalence Updater"),
                TALKTALK_EQUIVALENCE_REPETITION,
                jobsAtStartup
        );
        scheduleEquivalenceJob(
                publisherUpdateTask(ROVI_EN).withName("Rovi EN Equivalence Updater"),
                ROVI_EN_EQUIVALENCE_REPETITION,
                jobsAtStartup
        );
        scheduleEquivalenceJob(
                publisherUpdateTask(RTE).withName("RTE Equivalence Updater"),
                RTE_EQUIVALENCE_REPETITION,
                jobsAtStartup
        );
        scheduleEquivalenceJob(
                publisherUpdateTask(BT_VOD).withName("BT VOD Equivalence Updater"),
                BT_VOD_EQUIVALENCE_REPETITION,
                jobsAtStartup
        );
        scheduleEquivalenceJob(
                publisherUpdateTask(BT_TVE_VOD)
                        .withName("BT TVE VOD (prod, conf1) Equivalence Updater"),
                BT_VOD_EQUIVALENCE_REPETITION,
                jobsAtStartup
        );
        scheduleEquivalenceJob(
                new DeltaContentEquivalenceUpdateTask(
                        contentFinder,
                        RecoveringEquivalenceUpdater.create(contentResolver, equivUpdater),
                        ignored)
                        .forPublisher(AMAZON_UNBOX)
                        .forLast(new Period().withDays(2)) //i.e. since last repetition.
                        .withName("Amazon Unbox Equivalence Delta Updater (last 48h)"),
                AMAZON_EQUIVALENCE_DELTA_48H_REPETITION
        );
        scheduleEquivalenceJob(
                new DeltaContentEquivalenceUpdateTask(
                        contentFinder,
                        RecoveringEquivalenceUpdater.create(contentResolver, equivUpdater),
                        ignored)
                        .forPublisher(AMAZON_UNBOX)
                        .forLast(new Period().withDays(10))
                        .withName("Amazon Unbox Equivalence Delta Updater (last 240h)"),
                AMAZON_EQUIVALENCE_DELTA_240H_REPETITION
        );
        scheduleEquivalenceJob(
                publisherUpdateTask(AMAZON_UNBOX).withName("Amazon Unbox Equivalence Updater"),
                AMAZON_EQUIVALENCE_REPETITION,
                jobsAtStartup
        );
        scheduleEquivalenceJob(
                publisherUpdateTask(UKTV).withName("UKTV Equivalence Updater"),
                UKTV_EQUIVALENCE_REPETITION,
                jobsAtStartup
        );
        scheduleEquivalenceJob(
                publisherUpdateTask(WIKIPEDIA).withName("Wikipedia Equivalence Updater"),
                WIKIPEDIA_EQUIVALENCE_REPETITION,
                jobsAtStartup
        );
        scheduleEquivalenceJob(
                publisherUpdateTask(BBC_MUSIC).withName("Music Equivalence Updater"),
                BBC_MUSIC_EQUIVALENCE_REPETITION,
                jobsAtStartup
        );
        scheduleEquivalenceJob(
                publisherUpdateTask(PA).withName("PA Equivalence Updater"),
                RepetitionRules.NEVER,
                jobsAtStartup
        );
        scheduleEquivalenceJob(
                publisherUpdateTask(BBC).withName("BBC Equivalence Updater"),
                RepetitionRules.NEVER,
                jobsAtStartup
        );
        scheduleEquivalenceJob(
                publisherUpdateTask(C4_PMLSD).withName("C4 PMLSD Equivalence Updater"),
                RepetitionRules.NEVER,
                jobsAtStartup
        );
        scheduleEquivalenceJob(
                publisherUpdateTask(BBC_REDUX).withName("Redux Equivalence Updater"),
                RepetitionRules.NEVER,
                jobsAtStartup
        );
        scheduleEquivalenceJob(
                publisherUpdateTask(NETFLIX).withName("Netflix Equivalence Updater"),
                RepetitionRules.NEVER,
                jobsAtStartup
        );
        scheduleEquivalenceJob(
                publisherUpdateTask(YOUVIEW).withName("YouView Equivalence Updater"),
                RepetitionRules.NEVER,
                jobsAtStartup
        );
        scheduleEquivalenceJob(
                publisherUpdateTask(YOUVIEW_STAGE).withName("YouView Stage Equivalence Updater"),
                RepetitionRules.NEVER,
                jobsAtStartup
        );
        scheduleEquivalenceJob(
                publisherUpdateTask(VF_AE).withName("VF AE Equivalence Updater"),
                VF_AE_EQUIVALENCE_REPETITION,
                jobsAtStartup
        );
        scheduleEquivalenceJob(
                publisherUpdateTask(VF_BBC).withName("VF BBC Equivalence Updater"),
                VF_BBC_EQUIVALENCE_REPETITION,
                jobsAtStartup
        );
        scheduleEquivalenceJob(
                publisherUpdateTask(VF_C5).withName("VF C5 Equivalence Updater"),
                VF_C5_EQUIVALENCE_REPETITION,
                jobsAtStartup
        );
        scheduleEquivalenceJob(
                publisherUpdateTask(VF_ITV).withName("VF ITV Equivalence Updater"),
                VF_ITV_EQUIVALENCE_REPETITION,
                jobsAtStartup
        );
        scheduleEquivalenceJob(
                publisherUpdateTask(VF_VIACOM).withName("VF VIACOM Equivalence Updater"),
                VF_VIACOM_EQUIVALENCE_REPETITION,
                jobsAtStartup
        );
        scheduleEquivalenceJob(
                publisherUpdateTask(VF_VUBIQUITY).withName("VF VUBIQUITY Equivalence Updater"),
                VF_VUBIQUITY_EQUIVALENCE_REPETITION,
                jobsAtStartup
        );
        scheduleEquivalenceJob(
                publisherUpdateTask(EBMS_VF_UK).withName("EBMS VF Equivalence Updater"),
                EBMS_VF_EQUIVALENCE_REPETITION,
                jobsAtStartup
        );
        scheduleEquivalenceJob(
                publisherUpdateTask(REDBEE_MEDIA)
                        .withName("Redbee Statutory Listings Equivalence Updater"),
                REDBEE_MEDIA_EQUIVALENCE_REPETITION,
                jobsAtStartup
        );
        scheduleEquivalenceJob(
                publisherUpdateTask(AMC_EBS).withName("AMC EBS Equivalence Updater"),
                AMC_EBS_EQUIVALENCE_REPETITION,
                jobsAtStartup
        );
        scheduleEquivalenceJob(
                publisherUpdateTask(C4_PRESS).withName("C4 Press Equivalence Updater"),
                C4_PRESS_EQUIVALENCE_REPETITION,
                jobsAtStartup
        );
        scheduleEquivalenceJob(
                new DeltaContentEquivalenceUpdateTask(
                        contentFinder,
                        RecoveringEquivalenceUpdater.create(contentResolver, equivUpdater),
                        ignored)
                        .forPublisher(BARB_MASTER)
                        .forLast(new Period().withDays(3))
                        .withName("BARB CDMF Delta Updater (last 72h)"),
                CDMF_EQUIVALENCE_DELTA_REPETITION
        );
        scheduleEquivalenceJob(
                new DeltaContentEquivalenceUpdateTask(
                        contentFinder,
                        RecoveringEquivalenceUpdater.create(contentResolver, equivUpdater),
                        ignored)
                        .forPublisher(BARB_MASTER)
                        .forLast(new Period().withWeeks(2))
                        .withName("BARB CDMF Delta Updater (last 2 weeks)"),
                RepetitionRules.NEVER
        );
        scheduleEquivalenceJob(
                publisherUpdateTask(BARB_MASTER).withName("Barb CDMF Updater"),
                RepetitionRules.NEVER
        );
        scheduleEquivalenceJob(
                new DeltaContentEquivalenceUpdateTask(
                        contentFinder,
                        RecoveringEquivalenceUpdater.create(contentResolver, equivUpdater),
                        ignored)
                        .forPublisher(BARB_X_MASTER)
                        .forLast(new Period().withDays(3))
                        .withName("BARB XCDMF Delta Updater (last 72h)"),
                XCDMF_EQUIVALENCE_DELTA_REPETITION
        );
        scheduleEquivalenceJob(
                new DeltaContentEquivalenceUpdateTask(
                        contentFinder,
                        RecoveringEquivalenceUpdater.create(contentResolver, equivUpdater),
                        ignored)
                        .forPublisher(BARB_X_MASTER)
                        .forLast(new Period().withWeeks(2))
                        .withName("BARB XCDMF Delta Updater (last 2 weeks)"),
                RepetitionRules.NEVER
        );
        scheduleEquivalenceJob(
                publisherUpdateTask(BARB_X_MASTER).withName("Barb XCDMF Updater"),
                RepetitionRules.NEVER
        );
        scheduleEquivalenceJob(
                new DeltaContentEquivalenceUpdateTask(
                        contentFinder,
                        RecoveringEquivalenceUpdater.create(contentResolver, equivUpdater),
                        ignored)
                        .forPublisher(BARB_TRANSMISSIONS)
                        .forLast(new Period().withDays(3))
                        .withName("TxLog Delta Updater (last 72h)"),
                TXLOGS_EQUIVALENCE_DELTA_REPETITION
        );
        scheduleEquivalenceJob(
                new DeltaContentEquivalenceUpdateTask(
                        contentFinder,
                        RecoveringEquivalenceUpdater.create(contentResolver, equivUpdater),
                        ignored)
                        .forPublisher(BARB_TRANSMISSIONS)
                        .forLast(new Period().withWeeks(2))
                        .withName("TxLog Delta Updater (last 2 weeks)"),
                RepetitionRules.NEVER
        );
        scheduleEquivalenceJob(
                publisherUpdateTask(BARB_TRANSMISSIONS).withName("Barb TxLogs Updater"),
                RepetitionRules.NEVER
        );
        scheduleEquivalenceJob(
                new DeltaContentEquivalenceUpdateTask(
                        contentFinder,
                        RecoveringEquivalenceUpdater.create(contentResolver, equivUpdater),
                        ignored)
                        .forPublisher(LAYER3_TXLOGS)
                        .forLast(new Period().withDays(3))
                        .withName("Layer3 TxLog Delta Updater (last 72h)"),
                LAYER3_TXLOGS_EQUIVALENCE_DELTA_REPETITION
        );
        scheduleEquivalenceJob(
                new DeltaContentEquivalenceUpdateTask(
                        contentFinder,
                        RecoveringEquivalenceUpdater.create(contentResolver, equivUpdater),
                        ignored)
                        .forPublisher(LAYER3_TXLOGS)
                        .forLast(new Period().withWeeks(2))
                        .withName("Layer3 TxLog Delta Updater (last 2 weeks)"),
                RepetitionRules.NEVER
        );
        scheduleEquivalenceJob(
                publisherUpdateTask(LAYER3_TXLOGS).withName("Layer3 TxLogs Updater"),
                RepetitionRules.NEVER
        );
        scheduleEquivalenceJob(
                publisherUpdateTask(ITV_CPS).withName("ITV CPS Updater"),
                RepetitionRules.NEVER,
                jobsAtStartup
        );
        scheduleEquivalenceJob(
                taskBuilder(0, 7)
                        .withPublishers(BBC_NITRO)
                        .withChannelsSupplier(nitroChannelsSupplier())
                        .build().withName("BBC Nitro (8 day) Updater"),
                NITRO_EQUIVALENCE_REPETITION,
                jobsAtStartup
        );
        scheduleEquivalenceJob(
                taskBuilder(0, 7)
                        .withPublishers(BT_SPORT_EBS)
                        .withChannelsSupplier(youviewChannelsSupplier())
                        .build().withName("EBS Sports Equivalence (8 day) Updater"),
                EBS_SPORTS_EQUIVALENCE_REPETITION,
                jobsAtStartup
        );
        scheduleEquivalenceJob(
                taskBuilder(0, 7)
                        .withPublishers(BBC)
                        .withChannelsSupplier(bbcChannels())
                        .build().withName("BBC Schedule Equivalence (8 day) Updater"),
                BBC_SCHEDULE_EQUIVALENCE_REPETITION,
                jobsAtStartup
        );
        scheduleEquivalenceJob(
                taskBuilder(0, 7)
                        .withPublishers(ITV)
                        .withChannelsSupplier(itvChannels())
                        .build().withName("ITV Schedule Equivalence (8 day) Updater"),
                ITV_SCHEDULE_EQUIVALENCE_REPETITION,
                jobsAtStartup
        );
        scheduleEquivalenceJob(
                taskBuilder(0, 7)
                        .withPublishers(ITV_INTERLINKING)
                        .withChannelsSupplier(itvChannels())
                        .build().withName("ITV Interlinking Schedule Equivalence (8 day) Updater"),
                ITV_SCHEDULE_EQUIVALENCE_REPETITION,
                jobsAtStartup
        );
        scheduleEquivalenceJob(
                taskBuilder(0, 7)
                        .withPublishers(C4_PMLSD)
                        .withChannelsSupplier(c4Channels())
                        .build().withName("C4 Schedule Equivalence (8 day) Updater"),
                C4_SCHEDULE_EQUIVALENCE_REPETITION,
                jobsAtStartup
        );
        scheduleEquivalenceJob(
                taskBuilder(0, 7)
                        .withPublishers(FIVE)
                        .withChannelsSupplier(fiveChannels())
                        .build().withName("Five Schedule Equivalence (8 day) Updater"),
                RepetitionRules.NEVER,
                jobsAtStartup
        );
        scheduleEquivalenceJob(
                taskBuilder(7, 0)
                        .withPublishers(BBC_REDUX)
                        .withChannelsSupplier(bbcReduxChannels())
                        .build().withName("Redux Schedule Equivalence (8 day) Updater"),
                REDUX_SCHEDULE_EQUIVALENCE_REPETITION,
                jobsAtStartup
        );
        scheduleEquivalenceJob(
                publisherUpdateTask(IMDB_API).withName("IMDB Api Updater"),
                IMDB_API_EQUIVALENCE_REPETITION,
                jobsAtStartup
        );
        scheduleEquivalenceJob(
                publisherUpdateTask(C5_DATA_SUBMISSION).withName("C5 Data Submission Updater"),
                C5_DATA_SUBMISSION_EQUIVALENCE_REPETITION,
                jobsAtStartup
        );
        scheduleEquivalenceJob(
                new DeltaContentEquivalenceUpdateTask(
                        contentFinder,
                        RecoveringEquivalenceUpdater.create(contentResolver, equivUpdater),
                        ignored)
                        .forPublisher(BARB_CENSUS)
                        .forLast(new Period().withDays(3))
                        .withName("BARB Census Delta Updater (last 72h)"),
                BARB_CENSUS_EQUIVALENCE_DELTA_REPETITION
        );
        scheduleEquivalenceJob(
                new DeltaContentEquivalenceUpdateTask(
                        contentFinder,
                        RecoveringEquivalenceUpdater.create(contentResolver, equivUpdater),
                        ignored)
                        .forPublisher(BARB_CENSUS)
                        .forLast(new Period().withWeeks(2))
                        .withName("BARB Census Delta Updater (last 2 weeks)"),
                RepetitionRules.NEVER
        );
        scheduleEquivalenceJob(
                publisherUpdateTask(BARB_CENSUS).withName("BARB Census Updater"),
                RepetitionRules.NEVER,
                jobsAtStartup
        );
        scheduleEquivalenceJob(
                new DeltaContentEquivalenceUpdateTask(
                        contentFinder,
                        RecoveringEquivalenceUpdater.create(contentResolver, equivUpdater),
                        ignored
                )
                        .forPublisher(BARB_NLE)
                        .forLast(new Period().withDays(3))
                        .withName("BARB NLE Delta Updater (last 72h)"),
                BARB_NLE_EQUIVALENCE_DELTA_REPETITION
        );
        scheduleEquivalenceJob(
                new DeltaContentEquivalenceUpdateTask(
                        contentFinder,
                        RecoveringEquivalenceUpdater.create(contentResolver, equivUpdater),
                        ignored
                )
                        .forPublisher(BARB_NLE)
                        .forLast(new Period().withWeeks(2))
                        .withName("BARB NLE Delta Updater (last 2 weeks)"),
                RepetitionRules.NEVER
        );
        scheduleEquivalenceJob(
                publisherUpdateTask(BARB_NLE).withName("BARB NLE Updater"),
                RepetitionRules.NEVER,
                jobsAtStartup
        );
        scheduleEquivalenceJob(
                publisherUpdateTask(IMDB).withName("IMDB Updater"),
                IMDB_EQUIVALENCE_REPETITION,
                jobsAtStartup
        );
        scheduleEquivalenceJob(
                new DeltaContentEquivalenceUpdateTask(
                        contentFinder,
                        RecoveringEquivalenceUpdater.create(contentResolver, equivUpdater),
                        ignored
                )
                        .forPublisher(IMDB)
                        .forLast(new Period().withDays(3))
                        .withName("IMDB Delta Updater (last 72h)"),
                IMDB_EQUIVALENCE_DELTA_REPETITION
        );
    }

    private void addYouViewScheduleEquivalenceJobs(Set<ScheduledTask> jobsAtStartup) {
        // This job is scheduled for late in the day so run it for +8 days to ensure we get +7 day
        // coverage from the next day onwards
        scheduleEquivalenceJob(
                taskBuilder(0, 8)
                        .withPublishers(YOUVIEW)
                        .withChannelsSupplier(youviewChannelsSupplier())
                        .build().withName("YouView Schedule Equivalence (8 day) Updater"),
                YOUVIEW_SCHEDULE_EQUIVALENCE_REPETITION
        );
        scheduleEquivalenceJob(
                taskBuilder(0, 7)
                        .withPublishers(YOUVIEW_STAGE)
                        .withChannelsSupplier(youviewChannelsSupplier())
                        .build().withName("YouView Stage Schedule Equivalence (8 day) Updater"),
                YOUVIEW_STAGE_SCHEDULE_EQUIVALENCE_REPETITION
        );
        // This job is scheduled for late in the day so run it for +8 days to ensure we get +7 day
        // coverage from the next day onwards
        scheduleEquivalenceJob(
                taskBuilder(0, 8)
                        .withPublishers(YOUVIEW_BT)
                        .withChannelsSupplier(youviewChannelsSupplier())
                        .build().withName("YouView BT Schedule Equivalence (8 day) Updater"),
                YOUVIEW_SCHEDULE_EQUIVALENCE_REPETITION
        );
        scheduleEquivalenceJob(
                taskBuilder(0, 7)
                        .withPublishers(YOUVIEW_BT_STAGE)
                        .withChannelsSupplier(youviewChannelsSupplier())
                        .build()
                        .withName("YouView Stage BT Schedule Equivalence (8 day) Updater"),
                YOUVIEW_STAGE_SCHEDULE_EQUIVALENCE_REPETITION
        );
        // This job is scheduled for late in the day so run it for +8 days to ensure we get +7 day
        // coverage from the next day onwards
        scheduleEquivalenceJob(
                taskBuilder(0, 8)
                        .withPublishers(YOUVIEW_SCOTLAND_RADIO)
                        .withChannelsSupplier(youviewChannelsSupplier())
                        .build()
                        .withName("YouView Scotland Radio Schedule Equivalence (8 day) Updater"),
                YOUVIEW_SCHEDULE_EQUIVALENCE_REPETITION
        );
        scheduleEquivalenceJob(
                taskBuilder(0, 7)
                        .withPublishers(YOUVIEW_SCOTLAND_RADIO_STAGE)
                        .withChannelsSupplier(youviewChannelsSupplier())
                        .build()
                        .withName(
                                "YouView Stage Scotland Radio Schedule Equivalence (8 day) Updater"),
                YOUVIEW_STAGE_SCHEDULE_EQUIVALENCE_REPETITION
        );
    }

    private Set<ScheduledTask> scheduleEquivalenceJob(ScheduledTask task,
            RepetitionRule repetitionRule,
            Set<ScheduledTask> jobsAtStartup) {
        taskScheduler.schedule(task, repetitionRule);
        if (!repetitionRule.equals(RepetitionRules.NEVER)) {
            jobsAtStartup.add(task);
        }
        return jobsAtStartup;
    }

    private void scheduleEquivalenceJob(ScheduledTask task,
            RepetitionRule repetitionRule) {
        taskScheduler.schedule(task, repetitionRule);
    }

    private Builder taskBuilder(int back, int forward) {
        return ScheduleEquivalenceUpdateTask.builder()
                .withContentResolver(contentResolver)
                .withUpdater(equivUpdater)
                .withScheduleResolver(scheduleResolver)
                .withBack(back)
                .withForward(forward);
    }

    @Bean
    public MongoScheduleTaskProgressStore progressStore() {
        return new MongoScheduleTaskProgressStore(db);
    }

    private ContentEquivalenceUpdateTask publisherUpdateTask(final Publisher... publishers) {
        return new ContentEquivalenceUpdateTask(
                contentLister,
                contentResolver,
                getNewDefaultExecutor(),
                progressStore(),
                equivUpdater,
                ignored
        ).forPublishers(publishers);
    }

    private ExecutorService getNewDefaultExecutor(){
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                EQUIV_THREADS_PER_JOB, EQUIV_THREADS_PER_JOB, //this is used by all equiv tasks, so increase with caution.
                60, TimeUnit.SECONDS,
                new AlwaysBlockingQueue<>(SAVE_EVERY_BLOCK_SIZE)
        );
        executor.allowCoreThreadTimeOut(true);
        return executor;
    }

    //Controllers...
    @Bean
    public ContentEquivalenceUpdateController contentEquivalenceUpdateController() {
        return ContentEquivalenceUpdateController.create(equivUpdater, contentResolver, lookupStore);
    }

    @Bean
    public EquivalenceResultController resultEquivalenceResultController() {
        return new EquivalenceResultController(
                equivalenceResultStore,
                equivProbeStore(),
                contentResolver,
                lookupStore
        );
    }

    @Bean
    public RecentResultController recentEquivalenceResultController() {
        return new RecentResultController(equivalenceResultStore);
    }

    @Bean
    public EquivGraphController debugGraphController() {
        return EquivGraphController.create(lookupStore);
    }

    @Bean
    public RemoveEquivalenceController removeEquivalenceController() {
        return new RemoveEquivalenceController(equivalenceBreaker());
    }

    @Bean
    public EquivalenceBreaker equivalenceBreaker() {
        return EquivalenceBreaker.create(contentResolver, lookupStore, lookupWriter, explicitLookupWriter);
    }

    //Probes...
    @Bean
    public EquivalenceProbeStore equivProbeStore() {
        return new MongoEquivalenceProbeStore(db);
    }

    @Bean
    public EquivalenceResultProbeController equivProbeController() {
        return new EquivalenceResultProbeController(equivalenceResultStore, equivProbeStore());
    }

    private Supplier<Iterable<Channel>> bbcChannels() {
        return Suppliers.ofInstance(Iterables.transform(
                BbcIonServices.services.values(),
                new Function<String, Channel>() {

                    @Override
                    public Channel apply(String input) {
                        return channelResolver.fromUri(input).requireValue();
                    }
                }
        ));
    }

    private Supplier<Iterable<Channel>> youviewChannelsSupplier() {
        return new YouViewChannelsChannelSupplier(youviewChannelResolver);
    }

    private Supplier<Iterable<Channel>> nitroChannelsSupplier() {
        return Suppliers.ofInstance(
                (Iterable<Channel>) new NitroChannelMap(channelResolver).values()
        );
    }

    private Supplier<Iterable<Channel>> itvChannels() {
        return Suppliers.ofInstance(
                (Iterable<Channel>) new ItvWhatsonChannelMap(channelResolver).values()
        );
    }

    private Supplier<Iterable<Channel>> c4Channels() {
        return Suppliers.ofInstance(
                (Iterable<Channel>) new C4AtomApi(channelResolver).getChannelMap().values()
        );
    }

    private Supplier<Iterable<Channel>> fiveChannels() {
        return Suppliers.ofInstance(
                (Iterable<Channel>) new FiveChannelMap(channelResolver).values()
        );
    }

    private Supplier<Iterable<Channel>> bbcReduxChannels() {

        return new Supplier<Iterable<Channel>>() {

            @Override
            public Iterable<Channel> get() {
                return new ReduxServices(channelResolver).channelMap().values();
            }
        };
    }

    private EquivalenceUpdatingWorker equivUpdatingWorker() {
        return new EquivalenceUpdatingWorker(
                contentResolver,
                lookupStore,
                equivalenceResultStore,
                equivUpdater,
                Predicates.or(ImmutableList.<Predicate<? super Content>>of(
                        sourceIsIn(
                                BBC_REDUX,
                                YOUVIEW,
                                YOUVIEW_STAGE,
                                YOUVIEW_BT,
                                YOUVIEW_BT_STAGE,
                                BETTY,
                                BT_TVE_VOD,
                                BT_VOD,
                                BBC_NITRO
                        ),
                        Predicates.and(
                                Predicates.instanceOf(Container.class),
                                sourceIsIn(
                                        BBC,
                                        C4_PMLSD,
                                        ITV,
                                        FIVE,
                                        BBC_REDUX,
                                        ITUNES,
                                        RADIO_TIMES,
                                        LOVEFILM,
                                        TALK_TALK,
                                        YOUVIEW,
                                        NETFLIX,
                                        BBC_NITRO
                                )
                        )
                ))
        );
    }

    private Predicate<Content> sourceIsIn(Publisher... srcs) {
        final ImmutableSet<Publisher> sources = ImmutableSet.copyOf(srcs);
        return new Predicate<Content>() {

            @Override
            public boolean apply(Content input) {
                return sources.contains(input.getPublisher());
            }
        };
    }

    @Bean
    @Lazy()
    public KafkaConsumer equivalenceUpdatingMessageListener() {
        return messaging.messageConsumerFactory()
                .createConsumer(
                        equivUpdatingWorker(),
                        JacksonMessageSerializer.forType(EntityUpdatedMessage.class),
                        contentChanges,
                        "EquivUpdater"
                )
                .withDefaultConsumers(defaultStreamedEquivUpdateConsumers)
                .withMaxConsumers(maxStreamedEquivUpdateConsumers)
                .withPersistentRetryPolicy(db)
                .build();
    }

    @PostConstruct
    public void startConsumer() {
        if (streamedChangesUpdateEquiv) {
            KafkaConsumer consumer = equivalenceUpdatingMessageListener();
            consumer.addListener(new Listener() {

                @Override
                public void failed(State from, Throwable failure) {
                    log.warn("equiv update listener failed to transition from " + from, failure);
                }

                @Override
                public void running() {
                    log.info("equiv update listener running");
                }

            }, MoreExecutors.sameThreadExecutor());
            consumer.startAsync();
        }
    }

    private static class YouViewChannelsChannelSupplier implements Supplier<Iterable<Channel>> {

        private final YouViewChannelResolver youviewChannelResolver;

        public YouViewChannelsChannelSupplier(YouViewChannelResolver youviewChannelResolver) {
            this.youviewChannelResolver = checkNotNull(youviewChannelResolver);
        }

        @Override
        public Iterable<Channel> get() {
            return youviewChannelResolver.getAllChannels();
        }
    }
}

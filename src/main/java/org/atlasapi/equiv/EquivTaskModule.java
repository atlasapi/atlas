package org.atlasapi.equiv;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.atlasapi.media.entity.Publisher.AMAZON_UNBOX;
import static org.atlasapi.media.entity.Publisher.AMC_EBS;
import static org.atlasapi.media.entity.Publisher.BBC;
import static org.atlasapi.media.entity.Publisher.BBC_MUSIC;
import static org.atlasapi.media.entity.Publisher.BBC_REDUX;
import static org.atlasapi.media.entity.Publisher.BETTY;
import static org.atlasapi.media.entity.Publisher.BT_TVE_VOD_SYSTEST2_CONFIG_1;
import static org.atlasapi.media.entity.Publisher.BT_TVE_VOD_VOLD_CONFIG_1;
import static org.atlasapi.media.entity.Publisher.BT_TVE_VOD_VOLE_CONFIG_1;
import static org.atlasapi.media.entity.Publisher.BT_VOD;
import static org.atlasapi.media.entity.Publisher.BT_TVE_VOD;
import static org.atlasapi.media.entity.Publisher.C4;
import static org.atlasapi.media.entity.Publisher.C4_PMLSD;
import static org.atlasapi.media.entity.Publisher.EBMS_VF_UK;
import static org.atlasapi.media.entity.Publisher.FIVE;
import static org.atlasapi.media.entity.Publisher.ITUNES;
import static org.atlasapi.media.entity.Publisher.ITV;
import static org.atlasapi.media.entity.Publisher.ITV_INTERLINKING;
import static org.atlasapi.media.entity.Publisher.LOVEFILM;
import static org.atlasapi.media.entity.Publisher.NETFLIX;
import static org.atlasapi.media.entity.Publisher.PA;
import static org.atlasapi.media.entity.Publisher.RADIO_TIMES;
import static org.atlasapi.media.entity.Publisher.REDBEE_MEDIA;
import static org.atlasapi.media.entity.Publisher.ROVI_EN;
import static org.atlasapi.media.entity.Publisher.RTE;
import static org.atlasapi.media.entity.Publisher.TALK_TALK;
import static org.atlasapi.media.entity.Publisher.UKTV;
import static org.atlasapi.media.entity.Publisher.YOUVIEW;
import static org.atlasapi.media.entity.Publisher.YOUVIEW_BT;
import static org.atlasapi.media.entity.Publisher.YOUVIEW_BT_STAGE;
import static org.atlasapi.media.entity.Publisher.YOUVIEW_SCOTLAND_RADIO;
import static org.atlasapi.media.entity.Publisher.YOUVIEW_SCOTLAND_RADIO_STAGE;
import static org.atlasapi.media.entity.Publisher.YOUVIEW_STAGE;
import static org.atlasapi.media.entity.Publisher.WIKIPEDIA;
import static org.atlasapi.media.entity.Publisher.VF_BBC;
import static org.atlasapi.media.entity.Publisher.VF_C5;
import static org.atlasapi.media.entity.Publisher.VF_ITV;
import static org.atlasapi.media.entity.Publisher.VF_AE;
import static org.atlasapi.media.entity.Publisher.VF_VIACOM;
import static org.atlasapi.media.entity.Publisher.VF_VUBIQUITY;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.annotation.PostConstruct;

import org.atlasapi.equiv.results.persistence.RecentEquivalenceResultStore;
import org.atlasapi.equiv.results.probe.EquivalenceProbeStore;
import org.atlasapi.equiv.results.probe.EquivalenceResultProbeController;
import org.atlasapi.equiv.results.probe.MongoEquivalenceProbeStore;
import org.atlasapi.equiv.results.www.EquivGraphController;
import org.atlasapi.equiv.results.www.EquivalenceResultController;
import org.atlasapi.equiv.results.www.RecentResultController;
import org.atlasapi.equiv.update.EquivalenceUpdater;
import org.atlasapi.equiv.update.tasks.ContentEquivalenceUpdateTask;
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
import org.atlasapi.persistence.content.listing.ContentLister;
import org.atlasapi.persistence.lookup.LookupWriter;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;
import org.atlasapi.remotesite.bbc.ion.BbcIonServices;
import org.atlasapi.remotesite.channel4.C4AtomApi;
import org.atlasapi.remotesite.five.FiveChannelMap;
import org.atlasapi.remotesite.itv.whatson.ItvWhatsonChannelMap;
import org.atlasapi.remotesite.redux.ReduxServices;
import org.atlasapi.remotesite.youview.YouViewChannelResolver;
import org.atlasapi.remotesite.youview.YouViewCoreModule;

import com.google.api.client.util.Lists;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import org.joda.time.Duration;
import org.joda.time.LocalTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Lazy;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
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
import com.metabroadcast.common.time.DayOfWeek;

@Configuration
@Import({EquivModule.class, KafkaMessagingModule.class, YouViewCoreModule.class })
public class EquivTaskModule {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private static final Set<String> ignored = ImmutableSet.of("http://www.bbc.co.uk/programmes/b006mgyl"); 
//  private static final RepetitionRule EQUIVALENCE_REPETITION = RepetitionRules.daily(new LocalTime(9, 00));
    private static final RepetitionRule RT_EQUIVALENCE_REPETITION = RepetitionRules.daily(new LocalTime(7, 00));
    private static final RepetitionRule ITUNES_EQUIVALENCE_REPETITION = RepetitionRules.weekly(DayOfWeek.FRIDAY, new LocalTime(7, 00));
    private static final RepetitionRule TALKTALK_EQUIVALENCE_REPETITION = RepetitionRules.daily(new LocalTime(11, 15));
    private static final RepetitionRule YOUVIEW_EQUIVALENCE_REPETITION = RepetitionRules.daily(new LocalTime(15, 00));
    private static final RepetitionRule YOUVIEW_STAGE_EQUIVALENCE_REPETITION = RepetitionRules.daily(new LocalTime(8, 00));
    private static final RepetitionRule YOUVIEW_SCHEDULE_EQUIVALENCE_REPETITION = RepetitionRules.daily(new LocalTime(21, 30));
    private static final RepetitionRule YOUVIEW_STAGE_SCHEDULE_EQUIVALENCE_REPETITION = RepetitionRules.daily(new LocalTime(9, 00));
    private static final RepetitionRule BBC_SCHEDULE_EQUIVALENCE_REPETITION = RepetitionRules.daily(new LocalTime(9, 00));
    private static final RepetitionRule ITV_SCHEDULE_EQUIVALENCE_REPETITION = RepetitionRules.daily(new LocalTime(11, 00));
    private static final RepetitionRule ITV_EQUIVALENCE_REPETITION = RepetitionRules.daily(new LocalTime(12, 00));
    private static final RepetitionRule C4_SCHEDULE_EQUIVALENCE_REPETITION = RepetitionRules.daily(new LocalTime(15, 00));
    private static final RepetitionRule FIVE_SCHEDULE_EQUIVALENCE_REPETITION = RepetitionRules.daily(new LocalTime(17, 00));
    private static final RepetitionRule REDUX_SCHEDULE_EQUIVALENCE_REPETITION = RepetitionRules.daily(new LocalTime(7, 00));
    private static final RepetitionRule ROVI_EN_EQUIVALENCE_REPETITION = RepetitionRules.daily(new LocalTime(8, 00));
    private static final RepetitionRule RTE_EQUIVALENCE_REPETITION = RepetitionRules.daily(new LocalTime(22, 00));
    private static final RepetitionRule BT_VOD_EQUIVALENCE_REPETITION = RepetitionRules.daily(new LocalTime(3, 00));
    private static final RepetitionRule AMAZON_EQUIVALENCE_REPETITION = RepetitionRules.daily(new LocalTime(3, 00));
    private static final RepetitionRule UKTV_EQUIVALENCE_REPETITION = RepetitionRules.daily(new LocalTime(20, 00));
    private static final RepetitionRule WIKIPEDIA_EQUIVALENCE_REPETITION = RepetitionRules.daily(new LocalTime(18, 00));
    private static final RepetitionRule BBC_MUSIC_EQUIVALENCE_REPETITION = RepetitionRules.every(Duration.standardHours(6));
    private static final RepetitionRule VF_BBC_EQUIVALENCE_REPETITION = RepetitionRules.NEVER;
    private static final RepetitionRule VF_C5_EQUIVALENCE_REPETITION = RepetitionRules.NEVER;
    private static final RepetitionRule VF_ITV_EQUIVALENCE_REPETITION = RepetitionRules.NEVER;
    private static final RepetitionRule VF_AE_EQUIVALENCE_REPETITION = RepetitionRules.NEVER;
    private static final RepetitionRule VF_VIACOM_EQUIVALENCE_REPETITION = RepetitionRules.NEVER;
    private static final RepetitionRule VF_VUBIQUITY_EQUIVALENCE_REPETITION = RepetitionRules.NEVER;
    private static final RepetitionRule EBMS_VF_EQUIVALENCE_REPETITION = RepetitionRules.NEVER;
    private static final RepetitionRule REDBEE_MEDIA_EQUIVALENCE_REPETITION = RepetitionRules.NEVER;
    private static final RepetitionRule AMC_EBS_EQUIVALENCE_REPETITION = RepetitionRules.daily(new LocalTime(04, 00));

    private @Value("${equiv.updater.enabled}") String updaterEnabled;
    private @Value("${equiv.updater.youviewschedule.enabled}") String youViewScheduleUpdaterEnabled;
    private @Value("${equiv.stream-updater.enabled}") Boolean streamedChangesUpdateEquiv;
    private @Value("${equiv.stream-updater.consumers.default}") Integer defaultStreamedEquivUpdateConsumers;
    private @Value("${equiv.stream-updater.consumers.max}") Integer maxStreamedEquivUpdateConsumers;
    private @Value("${messaging.destination.content.changes}") String contentChanges;
    
    private @Autowired ContentLister contentLister;
    private @Autowired SimpleScheduler taskScheduler;
    private @Autowired ContentResolver contentResolver;
    private @Autowired DatabasedMongo db;
    private @Autowired LookupEntryStore lookupStore;
    private @Autowired ScheduleResolver scheduleResolver;
    private @Autowired ChannelResolver channelResolver;
    private @Autowired LookupWriter lookupWriter;
    private @Autowired YouViewChannelResolver youviewChannelResolver;
    
    private @Autowired @Qualifier("contentUpdater") EquivalenceUpdater<Content> equivUpdater;
    private @Autowired RecentEquivalenceResultStore equivalenceResultStore;
    
    private @Autowired KafkaMessagingModule messaging;

    private final int NUM_OF_THREADS_FOR_STARTUP_JOBS = 4;
    
    @PostConstruct
    public void scheduleUpdater() {
        ExecutorService executorService = Executors.newFixedThreadPool(
                NUM_OF_THREADS_FOR_STARTUP_JOBS);

        List<ScheduledTask> jobsAtStartup = Lists.newArrayList();

        if (Boolean.parseBoolean(youViewScheduleUpdaterEnabled)) {
            addYouViewScheduleEquivalenceJobs(jobsAtStartup);
        }

        if(Boolean.parseBoolean(updaterEnabled)) {
            addEquivalenceJobs(jobsAtStartup);
        }

        for (ScheduledTask scheduledTask : jobsAtStartup) {
            executorService.submit(scheduledTask);
        }
    }

    private void addEquivalenceJobs(List<ScheduledTask> jobsAtStartup) {
        scheduleEquivalenceJob(publisherUpdateTask(ITV).withName("ITV Equivalence Updater"), ITUNES_EQUIVALENCE_REPETITION, jobsAtStartup);
        scheduleEquivalenceJob(publisherUpdateTask(ITV_INTERLINKING).withName("ITV Interlinking Equivalence Updater"), ITV_EQUIVALENCE_REPETITION, jobsAtStartup);
        scheduleEquivalenceJob(publisherUpdateTask(FIVE).withName("Five Equivalence Updater"), FIVE_SCHEDULE_EQUIVALENCE_REPETITION, jobsAtStartup);
        scheduleEquivalenceJob(publisherUpdateTask(ITUNES).withName("Itunes Equivalence Updater"), ITUNES_EQUIVALENCE_REPETITION, jobsAtStartup);
        scheduleEquivalenceJob(publisherUpdateTask(RADIO_TIMES).withName("RT Equivalence Updater"), RT_EQUIVALENCE_REPETITION,jobsAtStartup);
        scheduleEquivalenceJob(publisherUpdateTask(TALK_TALK).withName("TalkTalk Equivalence Updater"), TALKTALK_EQUIVALENCE_REPETITION, jobsAtStartup);
        scheduleEquivalenceJob(publisherUpdateTask(ROVI_EN).withName("Rovi EN Equivalence Updater"), ROVI_EN_EQUIVALENCE_REPETITION, jobsAtStartup);
        scheduleEquivalenceJob(publisherUpdateTask(RTE).withName("RTE Equivalence Updater"), RTE_EQUIVALENCE_REPETITION, jobsAtStartup);
        scheduleEquivalenceJob(publisherUpdateTask(BT_VOD).withName("BT VOD Equivalence Updater"), BT_VOD_EQUIVALENCE_REPETITION, jobsAtStartup);
        scheduleEquivalenceJob(publisherUpdateTask(BT_TVE_VOD).withName("BT TVE VOD (prod, conf1) Equivalence Updater"), BT_VOD_EQUIVALENCE_REPETITION, jobsAtStartup);
        scheduleEquivalenceJob(publisherUpdateTask(BT_TVE_VOD_VOLD_CONFIG_1).withName("BT TVE VOD (vold, conf1) Equivalence Updater"), BT_VOD_EQUIVALENCE_REPETITION, jobsAtStartup);
        scheduleEquivalenceJob(publisherUpdateTask(BT_TVE_VOD_VOLE_CONFIG_1).withName("BT TVE VOD (vole, conf1) Equivalence Updater"), BT_VOD_EQUIVALENCE_REPETITION, jobsAtStartup);
        scheduleEquivalenceJob(publisherUpdateTask(BT_TVE_VOD_SYSTEST2_CONFIG_1).withName("BT TVE VOD (systest2, conf1) Equivalence Updater"), BT_VOD_EQUIVALENCE_REPETITION, jobsAtStartup);
        scheduleEquivalenceJob(publisherUpdateTask(AMAZON_UNBOX).withName("Amazon Unbox Equivalence Updater"), AMAZON_EQUIVALENCE_REPETITION, jobsAtStartup);
        scheduleEquivalenceJob(publisherUpdateTask(UKTV).withName("UKTV Equivalence Updater"), UKTV_EQUIVALENCE_REPETITION, jobsAtStartup);
        scheduleEquivalenceJob(publisherUpdateTask(WIKIPEDIA).withName("Wikipedia Equivalence Updater"), WIKIPEDIA_EQUIVALENCE_REPETITION, jobsAtStartup);
        scheduleEquivalenceJob(publisherUpdateTask(BBC_MUSIC).withName("Music Equivalence Updater"), BBC_MUSIC_EQUIVALENCE_REPETITION, jobsAtStartup);
        scheduleEquivalenceJob(publisherUpdateTask(PA).withName("PA Equivalence Updater"), RepetitionRules.NEVER, jobsAtStartup);
        scheduleEquivalenceJob(publisherUpdateTask(BBC).withName("BBC Equivalence Updater"), RepetitionRules.NEVER, jobsAtStartup);
        scheduleEquivalenceJob(publisherUpdateTask(C4).withName("C4 Equivalence Updater"), RepetitionRules.NEVER,jobsAtStartup);
        scheduleEquivalenceJob(publisherUpdateTask(C4_PMLSD).withName("C4 PMLSD Equivalence Updater"), RepetitionRules.NEVER, jobsAtStartup);
        scheduleEquivalenceJob(publisherUpdateTask(BBC_REDUX).withName("Redux Equivalence Updater"), RepetitionRules.NEVER, jobsAtStartup);
        //scheduleEquivalenceJob(publisherUpdateTask(LOVEFILM).withName("Lovefilm Equivalence Updater"), RepetitionRules.every(Duration.standardHours(12)).withOffset(Duration.standardHours(10)), jobsAtStartup);
        scheduleEquivalenceJob(publisherUpdateTask(NETFLIX).withName("Netflix Equivalence Updater"), RepetitionRules.NEVER, jobsAtStartup);
        scheduleEquivalenceJob(publisherUpdateTask(YOUVIEW).withName("YouView Equivalence Updater"), RepetitionRules.NEVER, jobsAtStartup);
        scheduleEquivalenceJob(publisherUpdateTask(YOUVIEW_STAGE).withName("YouView Stage Equivalence Updater"), RepetitionRules.NEVER, jobsAtStartup);
        scheduleEquivalenceJob(publisherUpdateTask(VF_AE).withName("VF AE Equivalence Updater"), VF_AE_EQUIVALENCE_REPETITION, jobsAtStartup);
        scheduleEquivalenceJob(publisherUpdateTask(VF_BBC).withName("VF BBC Equivalence Updater"), VF_BBC_EQUIVALENCE_REPETITION, jobsAtStartup);
        scheduleEquivalenceJob(publisherUpdateTask(VF_C5).withName("VF C5 Equivalence Updater"), VF_C5_EQUIVALENCE_REPETITION, jobsAtStartup);
        scheduleEquivalenceJob(publisherUpdateTask(VF_ITV).withName("VF ITV Equivalence Updater"), VF_ITV_EQUIVALENCE_REPETITION, jobsAtStartup);
        scheduleEquivalenceJob(publisherUpdateTask(VF_VIACOM).withName("VF VIACOM Equivalence Updater"), VF_VIACOM_EQUIVALENCE_REPETITION, jobsAtStartup);
        scheduleEquivalenceJob(publisherUpdateTask(VF_VUBIQUITY).withName("VF VUBIQUITY Equivalence Updater"), VF_VUBIQUITY_EQUIVALENCE_REPETITION, jobsAtStartup);
        scheduleEquivalenceJob(publisherUpdateTask(EBMS_VF_UK).withName("EBMS VF Equivalence Updater"), EBMS_VF_EQUIVALENCE_REPETITION, jobsAtStartup);
        scheduleEquivalenceJob(publisherUpdateTask(REDBEE_MEDIA).withName("Redbee Statutory Listings Equivalence Updater"), REDBEE_MEDIA_EQUIVALENCE_REPETITION, jobsAtStartup);
        scheduleEquivalenceJob(publisherUpdateTask(AMC_EBS).withName("AMC EBS Equivalence Updater"), AMC_EBS_EQUIVALENCE_REPETITION, jobsAtStartup);

        scheduleEquivalenceJob(taskBuilder(0, 7)
                        .withPublishers(BBC)
                        .withChannelsSupplier(bbcChannels())
                        .build().withName("BBC Schedule Equivalence (8 day) Updater"),
                BBC_SCHEDULE_EQUIVALENCE_REPETITION,
                jobsAtStartup);
        scheduleEquivalenceJob(taskBuilder(0, 7)
                        .withPublishers(ITV)
                        .withChannelsSupplier(itvChannels())
                        .build().withName("ITV Schedule Equivalence (8 day) Updater"),
                ITV_SCHEDULE_EQUIVALENCE_REPETITION,
                jobsAtStartup);
        scheduleEquivalenceJob(taskBuilder(0, 7)
                        .withPublishers(ITV_INTERLINKING)
                        .withChannelsSupplier(itvChannels())
                        .build().withName("ITV Interlinking Schedule Equivalence (8 day) Updater"),
                ITV_SCHEDULE_EQUIVALENCE_REPETITION,
                jobsAtStartup);
        scheduleEquivalenceJob(taskBuilder(0, 7)
                        .withPublishers(C4)
                        .withChannelsSupplier(c4Channels())
                        .build().withName("C4 Schedule Equivalence (8 day) Updater"),
                C4_SCHEDULE_EQUIVALENCE_REPETITION,
                jobsAtStartup);
        scheduleEquivalenceJob(taskBuilder(0, 7)
                        .withPublishers(C4_PMLSD)
                        .withChannelsSupplier(c4Channels())
                        .build().withName("C4 Schedule Equivalence (8 day) Updater"),
                C4_SCHEDULE_EQUIVALENCE_REPETITION,
                jobsAtStartup);
        scheduleEquivalenceJob(taskBuilder(0, 7)
                        .withPublishers(FIVE)
                        .withChannelsSupplier(fiveChannels())
                        .build().withName("Five Schedule Equivalence (8 day) Updater"),
                RepetitionRules.NEVER,
                jobsAtStartup);
        scheduleEquivalenceJob(taskBuilder(7, 0)
                        .withPublishers(BBC_REDUX)
                        .withChannelsSupplier(bbcReduxChannels())
                        .build().withName("Redux Schedule Equivalence (8 day) Updater"),
                REDUX_SCHEDULE_EQUIVALENCE_REPETITION,
                jobsAtStartup);

    }

    private void addYouViewScheduleEquivalenceJobs(List<ScheduledTask> jobsAtStartup) {

        scheduleEquivalenceJob(
                taskBuilder(0, 7)
                        .withPublishers(YOUVIEW)
                        .withChannelsSupplier(youviewChannelsSupplier())
                        .build().withName("YouView Schedule Equivalence (8 day) Updater"),
                YOUVIEW_SCHEDULE_EQUIVALENCE_REPETITION,
                jobsAtStartup
        );
        scheduleEquivalenceJob(
                taskBuilder(0, 7)
                        .withPublishers(YOUVIEW_STAGE)
                        .withChannelsSupplier(youviewChannelsSupplier())
                        .build().withName("YouView Stage Schedule Equivalence (8 day) Updater"),
                YOUVIEW_STAGE_SCHEDULE_EQUIVALENCE_REPETITION,
                jobsAtStartup
        );
        scheduleEquivalenceJob(
                taskBuilder(0, 7)
                        .withPublishers(YOUVIEW_BT)
                        .withChannelsSupplier(youviewChannelsSupplier())
                        .build().withName("YouView BT Schedule Equivalence (8 day) Updater"),
                YOUVIEW_SCHEDULE_EQUIVALENCE_REPETITION,
                jobsAtStartup
        );
        scheduleEquivalenceJob(
                taskBuilder(0, 7)
                        .withPublishers(YOUVIEW_BT_STAGE)
                        .withChannelsSupplier(youviewChannelsSupplier())
                        .build()
                        .withName("YouView Stage BT Schedule Equivalence (8 day) Updater"),
                YOUVIEW_STAGE_SCHEDULE_EQUIVALENCE_REPETITION,
                jobsAtStartup
        );
        scheduleEquivalenceJob(
                taskBuilder(0, 7)
                        .withPublishers(YOUVIEW_SCOTLAND_RADIO)
                        .withChannelsSupplier(youviewChannelsSupplier())
                        .build()
                        .withName("YouView Scotland Radio Schedule Equivalence (8 day) Updater"),
                YOUVIEW_SCHEDULE_EQUIVALENCE_REPETITION,
                jobsAtStartup
        );
        scheduleEquivalenceJob(
                taskBuilder(0, 7)
                        .withPublishers(YOUVIEW_SCOTLAND_RADIO_STAGE)
                        .withChannelsSupplier(youviewChannelsSupplier())
                        .build()
                        .withName(
                                "YouView Stage Scotland Radio Schedule Equivalence (8 day) Updater"),
                YOUVIEW_STAGE_SCHEDULE_EQUIVALENCE_REPETITION,
                jobsAtStartup
        );
    }

    private List<ScheduledTask> scheduleEquivalenceJob(ScheduledTask task,
            RepetitionRule repetitionRule,
            List<ScheduledTask> jobsAtStartup) {
        taskScheduler.schedule(task, repetitionRule);
        if (!repetitionRule.equals(RepetitionRules.NEVER)) {
            jobsAtStartup.add(task);
        }
        return jobsAtStartup;
    }

    private Builder taskBuilder(int back, int forward) {
        return ScheduleEquivalenceUpdateTask.builder()
            .withUpdater(equivUpdater)
            .withScheduleResolver(scheduleResolver)
            .withBack(back)
            .withForward(forward);
    }

    public @Bean MongoScheduleTaskProgressStore progressStore() {
        return new MongoScheduleTaskProgressStore(db);
    }
    
    private ContentEquivalenceUpdateTask publisherUpdateTask(final Publisher... publishers) {
        return new ContentEquivalenceUpdateTask(contentLister, contentResolver, progressStore(), equivUpdater, ignored).forPublishers(publishers);
    }
    
    //Controllers...
    public @Bean ContentEquivalenceUpdateController contentEquivalenceUpdateController() {
        return new ContentEquivalenceUpdateController(equivUpdater, contentResolver, lookupStore);
    }
    
    public @Bean EquivalenceResultController resultEquivalenceResultController() {
        return new EquivalenceResultController(equivalenceResultStore, equivProbeStore(), contentResolver, lookupStore);
    }
    
    public @Bean RecentResultController recentEquivalenceResultController() {
        return new RecentResultController(equivalenceResultStore);
    }
    
    public @Bean EquivGraphController debugGraphController() {
        return new EquivGraphController(lookupStore);
    }
    
    public @Bean RemoveEquivalenceController removeEquivalenceController() {
        return new RemoveEquivalenceController(new EquivalenceBreaker(contentResolver, lookupStore, lookupWriter));
    }
    
    //Probes...
    public @Bean EquivalenceProbeStore equivProbeStore() { 
        return new MongoEquivalenceProbeStore(db);
    }
    
    public @Bean EquivalenceResultProbeController equivProbeController() {
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
        return new EquivalenceUpdatingWorker(contentResolver, lookupStore, equivalenceResultStore, equivUpdater,
            Predicates.or(ImmutableList.<Predicate<? super Content>>of(
                sourceIsIn(BBC_REDUX, YOUVIEW, YOUVIEW_STAGE, YOUVIEW_BT, YOUVIEW_BT_STAGE, BETTY, BT_TVE_VOD, BT_VOD),
                Predicates.and(Predicates.instanceOf(Container.class),
                    sourceIsIn(BBC, C4, C4_PMLSD, ITV, FIVE, BBC_REDUX, ITUNES, 
                        RADIO_TIMES, LOVEFILM, TALK_TALK, YOUVIEW, NETFLIX))
            ))
        );
    }

    private Predicate<Content> sourceIsIn(Publisher... srcs) {
        final ImmutableSet<Publisher> sources = ImmutableSet.copyOf(srcs);
        return new Predicate<Content>(){
            @Override
            public boolean apply(Content input) {
                return sources.contains(input.getPublisher());
            }
        };
    }

    @Bean
    @Lazy(true)
    public Optional<KafkaConsumer> equivalenceUpdatingMessageListener() {
        if (streamedChangesUpdateEquiv) {
            return Optional.of(messaging.messageConsumerFactory().createConsumer(
                    equivUpdatingWorker(), JacksonMessageSerializer.forType(EntityUpdatedMessage.class), 
                    contentChanges, "EquivUpdater")
                .withDefaultConsumers(defaultStreamedEquivUpdateConsumers)
                .withMaxConsumers(maxStreamedEquivUpdateConsumers)
                .build());
        } else {
            return Optional.absent();
        }
    }
    
    @PostConstruct
    public void startConsumer() {
        Optional<KafkaConsumer> consumer = equivalenceUpdatingMessageListener();
        if (consumer.isPresent()) {
            consumer.get().addListener(new Listener() {
                @Override
                public void failed(State from, Throwable failure) {
                    log.warn("equiv update listener failed to transition from " + from, failure);
                }
                @Override
                public void running() {
                    log.info("equiv update listener running");
                }
                
            }, MoreExecutors.sameThreadExecutor());
            consumer.get().startAsync();
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

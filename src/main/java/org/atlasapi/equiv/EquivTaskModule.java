package org.atlasapi.equiv;

import static org.atlasapi.media.entity.Publisher.BBC;
import static org.atlasapi.media.entity.Publisher.BBC_REDUX;
import static org.atlasapi.media.entity.Publisher.BETTY;
import static org.atlasapi.media.entity.Publisher.BT_VOD;
import static org.atlasapi.media.entity.Publisher.C4;
import static org.atlasapi.media.entity.Publisher.C4_PMLSD;
import static org.atlasapi.media.entity.Publisher.FIVE;
import static org.atlasapi.media.entity.Publisher.ITUNES;
import static org.atlasapi.media.entity.Publisher.ITV;
import static org.atlasapi.media.entity.Publisher.LOVEFILM;
import static org.atlasapi.media.entity.Publisher.NETFLIX;
import static org.atlasapi.media.entity.Publisher.PA;
import static org.atlasapi.media.entity.Publisher.RADIO_TIMES;
import static org.atlasapi.media.entity.Publisher.ROVI_EN;
import static org.atlasapi.media.entity.Publisher.RTE;
import static org.atlasapi.media.entity.Publisher.TALK_TALK;
import static org.atlasapi.media.entity.Publisher.YOUVIEW;
import static org.atlasapi.media.entity.Publisher.YOUVIEW_BT;
import static org.atlasapi.media.entity.Publisher.YOUVIEW_BT_STAGE;
import static org.atlasapi.media.entity.Publisher.YOUVIEW_SCOTLAND_RADIO;
import static org.atlasapi.media.entity.Publisher.YOUVIEW_SCOTLAND_RADIO_STAGE;
import static org.atlasapi.media.entity.Publisher.YOUVIEW_STAGE;

import java.util.Set;

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
import org.atlasapi.remotesite.youview.DefaultYouViewChannelResolver;
import org.atlasapi.remotesite.youview.YouViewChannelResolver;
import org.atlasapi.remotesite.youview.YouViewCoreModule;
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
import com.metabroadcast.common.scheduling.SimpleScheduler;

@Configuration
@Import({EquivModule.class, KafkaMessagingModule.class, YouViewCoreModule.class })
public class EquivTaskModule {
    
    private final Logger log = LoggerFactory.getLogger(getClass());

    private static final Set<String> ignored = ImmutableSet.of("http://www.bbc.co.uk/programmes/b006mgyl"); 
//  private static final RepetitionRule EQUIVALENCE_REPETITION = RepetitionRules.daily(new LocalTime(9, 00));
    private static final RepetitionRule RT_EQUIVALENCE_REPETITION = RepetitionRules.daily(new LocalTime(7, 00));
    private static final RepetitionRule TALKTALK_EQUIVALENCE_REPETITION = RepetitionRules.daily(new LocalTime(11, 15));
    private static final RepetitionRule YOUVIEW_EQUIVALENCE_REPETITION = RepetitionRules.daily(new LocalTime(15, 00));
    private static final RepetitionRule YOUVIEW_STAGE_EQUIVALENCE_REPETITION = RepetitionRules.daily(new LocalTime(8, 00));
    private static final RepetitionRule YOUVIEW_SCHEDULE_EQUIVALENCE_REPETITION = RepetitionRules.daily(new LocalTime(13, 00));
    private static final RepetitionRule YOUVIEW_STAGE_SCHEDULE_EQUIVALENCE_REPETITION = RepetitionRules.daily(new LocalTime(9, 00));
    private static final RepetitionRule BBC_SCHEDULE_EQUIVALENCE_REPETITION = RepetitionRules.daily(new LocalTime(9, 00));
    private static final RepetitionRule ITV_SCHEDULE_EQUIVALENCE_REPETITION = RepetitionRules.daily(new LocalTime(11, 00));
    private static final RepetitionRule ITV_EQUIVALENCE_REPETITION = RepetitionRules.daily(new LocalTime(12, 00));
    private static final RepetitionRule C4_SCHEDULE_EQUIVALENCE_REPETITION = RepetitionRules.daily(new LocalTime(15, 00));
    private static final RepetitionRule FIVE_SCHEDULE_EQUIVALENCE_REPETITION = RepetitionRules.daily(new LocalTime(17, 00));
    private static final RepetitionRule REDUX_SCHEDULE_EQUIVALENCE_REPETITION = RepetitionRules.daily(new LocalTime(7, 00));
    private static final RepetitionRule ROVI_EN_EQUIVALENCE_REPETITION = RepetitionRules.daily(new LocalTime(8, 00));
    private static final RepetitionRule RTE_EQUIVALENCE_REPETITION = RepetitionRules.daily(new LocalTime(22, 00));
    private static final RepetitionRule BT_VOD_EQUIVALENCE_REPETITION = RepetitionRules.NEVER;
    
    private @Value("${equiv.updater.enabled}") String updaterEnabled;
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
    
    @PostConstruct
    public void scheduleUpdater() {
        if(Boolean.parseBoolean(updaterEnabled)) {
            taskScheduler.schedule(publisherUpdateTask(PA).withName("PA Equivalence Updater"), RepetitionRules.NEVER);
            taskScheduler.schedule(publisherUpdateTask(BBC).withName("BBC Equivalence Updater"), RepetitionRules.NEVER);
            taskScheduler.schedule(publisherUpdateTask(C4).withName("C4 Equivalence Updater"), RepetitionRules.NEVER);
            taskScheduler.schedule(publisherUpdateTask(C4_PMLSD).withName("C4 PMLSD Equivalence Updater"), RepetitionRules.NEVER);
            taskScheduler.schedule(publisherUpdateTask(ITV).withName("ITV Equivalence Updater"), ITV_EQUIVALENCE_REPETITION);
            taskScheduler.schedule(publisherUpdateTask(FIVE).withName("Five Equivalence Updater"), FIVE_SCHEDULE_EQUIVALENCE_REPETITION);
            taskScheduler.schedule(publisherUpdateTask(BBC_REDUX).withName("Redux Equivalence Updater"), RepetitionRules.NEVER);
            taskScheduler.schedule(publisherUpdateTask(ITUNES).withName("Itunes Equivalence Updater"), RepetitionRules.NEVER);
            taskScheduler.schedule(publisherUpdateTask(RADIO_TIMES).withName("RT Equivalence Updater"), RT_EQUIVALENCE_REPETITION);
            //taskScheduler.schedule(publisherUpdateTask(LOVEFILM).withName("Lovefilm Equivalence Updater"), RepetitionRules.every(Duration.standardHours(12)).withOffset(Duration.standardHours(10)));
            taskScheduler.schedule(publisherUpdateTask(NETFLIX).withName("Netflix Equivalence Updater"), RepetitionRules.NEVER);
            taskScheduler.schedule(publisherUpdateTask(YOUVIEW).withName("YouView Equivalence Updater"), RepetitionRules.NEVER);
            taskScheduler.schedule(publisherUpdateTask(YOUVIEW_STAGE).withName("YouView Stage Equivalence Updater"), RepetitionRules.NEVER);
            taskScheduler.schedule(publisherUpdateTask(TALK_TALK).withName("TalkTalk Equivalence Updater"), TALKTALK_EQUIVALENCE_REPETITION);
            taskScheduler.schedule(publisherUpdateTask(ROVI_EN).withName("Rovi EN Equivalence Updater"), ROVI_EN_EQUIVALENCE_REPETITION);
            taskScheduler.schedule(publisherUpdateTask(RTE).withName("RTE Equivalence Updater"), RTE_EQUIVALENCE_REPETITION);
            taskScheduler.schedule(publisherUpdateTask(BT_VOD).withName("BT VOD Equivalence Updater"), BT_VOD_EQUIVALENCE_REPETITION);
            
            taskScheduler.schedule(publisherUpdateTask(Publisher.BBC_MUSIC).withName("Music Equivalence Updater"), RepetitionRules.every(Duration.standardHours(6)));
            
            taskScheduler.schedule(taskBuilder(0, 7)
                    .withPublishers(YOUVIEW)
                    .withChannels(youviewChannelResolver.getAllChannels())
                    .build().withName("YouView Schedule Equivalence (8 day) Updater"), 
                YOUVIEW_SCHEDULE_EQUIVALENCE_REPETITION);
            taskScheduler.schedule(taskBuilder(0, 7)
                    .withPublishers(YOUVIEW_STAGE)
                    .withChannels(youviewChannelResolver.getAllChannels())
                    .build().withName("YouView Stage Schedule Equivalence (8 day) Updater"), 
                YOUVIEW_STAGE_SCHEDULE_EQUIVALENCE_REPETITION);
            taskScheduler.schedule(taskBuilder(0, 7)
                    .withPublishers(YOUVIEW_BT)
                    .withChannels(youviewChannelResolver.getAllChannels())
                    .build().withName("YouView BT Schedule Equivalence (8 day) Updater"), 
                YOUVIEW_SCHEDULE_EQUIVALENCE_REPETITION);
            taskScheduler.schedule(taskBuilder(0, 7)
                    .withPublishers(YOUVIEW_BT_STAGE)
                    .withChannels(youviewChannelResolver.getAllChannels())
                    .build().withName("YouView Stage BT Schedule Equivalence (8 day) Updater"), 
                YOUVIEW_STAGE_SCHEDULE_EQUIVALENCE_REPETITION);
            taskScheduler.schedule(taskBuilder(0, 7)
                    .withPublishers(YOUVIEW_SCOTLAND_RADIO)
                    .withChannels(youviewChannelResolver.getAllChannels())
                    .build().withName("YouView Scotland Radio Schedule Equivalence (8 day) Updater"), 
                YOUVIEW_SCHEDULE_EQUIVALENCE_REPETITION);
            taskScheduler.schedule(taskBuilder(0, 7)
                    .withPublishers(YOUVIEW_SCOTLAND_RADIO_STAGE)
                    .withChannels(youviewChannelResolver.getAllChannels())
                    .build().withName("YouView Stage Scotland Radio Schedule Equivalence (8 day) Updater"), 
                YOUVIEW_STAGE_SCHEDULE_EQUIVALENCE_REPETITION);
            taskScheduler.schedule(taskBuilder(0, 7)
                    .withPublishers(BBC)
                    .withChannels(bbcChannels())
                    .build().withName("BBC Schedule Equivalence (8 day) Updater"), 
                BBC_SCHEDULE_EQUIVALENCE_REPETITION);
            taskScheduler.schedule(taskBuilder(0, 7)
                    .withPublishers(ITV)
                    .withChannels(itvChannels())
                    .build().withName("ITV Schedule Equivalence (8 day) Updater"), 
                ITV_SCHEDULE_EQUIVALENCE_REPETITION);
            taskScheduler.schedule(taskBuilder(0, 7)
                    .withPublishers(C4)
                    .withChannels(c4Channels())
                    .build().withName("C4 Schedule Equivalence (8 day) Updater"), 
                C4_SCHEDULE_EQUIVALENCE_REPETITION);
            taskScheduler.schedule(taskBuilder(0, 7)
                    .withPublishers(C4_PMLSD)
                    .withChannels(c4Channels())
                    .build().withName("C4 Schedule Equivalence (8 day) Updater"), 
                C4_SCHEDULE_EQUIVALENCE_REPETITION);
            taskScheduler.schedule(taskBuilder(0, 7)
                    .withPublishers(FIVE)
                    .withChannels(fiveChannels())
                    .build().withName("Five Schedule Equivalence (8 day) Updater"), 
                RepetitionRules.NEVER);
            taskScheduler.schedule(taskBuilder(7, 0)
                    .withPublishers(BBC_REDUX)
                    .withChannels(bbcReduxChannels())
                    .build().withName("Redux Schedule Equivalence (8 day) Updater"), 
                REDUX_SCHEDULE_EQUIVALENCE_REPETITION);
        }
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
        return new ContentEquivalenceUpdateController(equivUpdater, contentResolver);
    }
    
    public @Bean EquivalenceResultController resultEquivalenceResultController() {
        return new EquivalenceResultController(equivalenceResultStore, equivProbeStore(), contentResolver);
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
    
    private Iterable<Channel> bbcChannels() {
        return Iterables.transform(BbcIonServices.services.values(),
            new Function<String, Channel>() {
                @Override
                public Channel apply(String input) {
                    return channelResolver.fromUri(input).requireValue();
                }
            }
        );
    }

    private Iterable<Channel> itvChannels() {
        return new ItvWhatsonChannelMap(channelResolver).values();
    }
        
    private Iterable<Channel> c4Channels() {
        return new C4AtomApi(channelResolver).getChannelMap().values();
    }
    
    private Iterable<Channel> fiveChannels() {
        return new FiveChannelMap(channelResolver).values();
    }

    private Iterable<Channel> bbcReduxChannels() {
        return new ReduxServices(channelResolver).channelMap().values();
    }
    
    private EquivalenceUpdatingWorker equivUpdatingWorker() {
        return new EquivalenceUpdatingWorker(contentResolver, lookupStore, equivalenceResultStore, equivUpdater,
            Predicates.or(ImmutableList.<Predicate<? super Content>>of(
                sourceIsIn(BBC_REDUX, YOUVIEW, YOUVIEW_STAGE, YOUVIEW_BT, YOUVIEW_BT_STAGE, BETTY),
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
    
}

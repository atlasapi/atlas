package org.atlasapi.remotesite.bbc.nitro;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import javax.annotation.PostConstruct;

import org.atlasapi.AtlasMain;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelQuery;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.channel.ChannelWriter;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.ScheduleEntry.ItemRefAndBroadcast;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.ScheduleResolver;
import org.atlasapi.persistence.content.people.QueuingPersonWriter;
import org.atlasapi.persistence.content.schedule.mongo.ScheduleWriter;
import org.atlasapi.remotesite.bbc.nitro.channels.ChannelIngestTask;
import org.atlasapi.remotesite.bbc.nitro.channels.NitroChannelHydrator;
import org.atlasapi.remotesite.channel4.pmlsd.epg.ScheduleResolverBroadcastTrimmer;
import org.atlasapi.util.GroupLock;

import com.metabroadcast.atlas.glycerin.Glycerin;
import com.metabroadcast.atlas.glycerin.XmlGlycerin;
import com.metabroadcast.atlas.glycerin.XmlGlycerin.Builder;
import com.metabroadcast.common.scheduling.RepetitionRules;
import com.metabroadcast.common.scheduling.ScheduledTask;
import com.metabroadcast.common.scheduling.SimpleScheduler;
import com.metabroadcast.common.time.SystemClock;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.joda.time.Duration;
import org.joda.time.LocalDate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class BbcNitroModule {

    private static final Logger log = LoggerFactory.getLogger(BbcNitroModule.class);

    private @Value("${updaters.bbcnitro.enabled}") Boolean tasksEnabled;
    private @Value("${updaters.bbcnitro.offschedule.enabled}") Boolean offScheduleIngestEnabled;
    private @Value("${bbc.nitro.root}") String nitroRoot;
    private @Value("${bbc.nitro.apiKey}") String nitroApiKey;
    private @Value("${bbc.nitro.requestsPerSecond.today}") Integer nitroTodayRateLimit;
    private @Value("${bbc.nitro.requestsPerSecond.fortnight}") Integer nitroFortnightRateLimit;
    private @Value("${bbc.nitro.requestsPerSecond.threeweek}") Integer nitroThreeWeekRateLimit;
    private @Value("${bbc.nitro.requestsPerSecond.aroundtoday}") Integer nitroAroundTodayRateLimit;
    private @Value("${bbc.nitro.threadCount.today}") Integer nitroTodayThreadCount;
    private @Value("${bbc.nitro.threadCount.fortnight}") Integer nitroFortnightThreadCount;
    private @Value("${bbc.nitro.threadCount.threeweek}") Integer nitroThreeWeekThreadCount;
    private @Value("${bbc.nitro.threadCount.aroundtoday}") Integer nitroAroundTodayThreadCount;
    private @Value("${bbc.nitro.requestPageSize}") Integer nitroRequestPageSize;
    private @Value("${bbc.nitro.jobFailureThresholdPercent}") Integer jobFailureThresholdPercent;
    
    private @Autowired SimpleScheduler scheduler;
    private @Autowired ContentWriter contentWriter;
    private @Autowired ContentResolver contentResolver;
    private @Autowired ScheduleResolver scheduleResolver;
    private @Autowired ScheduleWriter scheduleWriter;
    private @Autowired ChannelResolver channelResolver;
    private @Autowired ChannelWriter channelWriter;
    private @Autowired QueuingPersonWriter peopleWriter;
    
    private final ThreadFactory nitroThreadFactory = new ThreadFactoryBuilder()
            .setNameFormat("nitro %s")
            .build();
    private final GroupLock<String> pidLock = GroupLock.natural();
    private final MetricRegistry metricRegistry = AtlasMain.metrics;

    @PostConstruct
    public void configure() {
        if (tasksEnabled) {
            scheduler.schedule(
                    nitroScheduleUpdateTask(
                            7,
                            7,
                            nitroFortnightThreadCount,
                            nitroFortnightRateLimit,
                            Optional.absent(),
                            "Nitro 15 day updater",
                            "nitro.15DayUpdater."
                    ),
                    RepetitionRules.every(Duration.standardHours(2))
            );
            scheduler.schedule(
                    nitroScheduleUpdateTask(
                            0,
                            0,
                            nitroTodayThreadCount,
                            nitroTodayRateLimit,
                            Optional.absent(),
                            "Nitro today updater",
                            "nitro.todayUpdater."
                    ),
                    RepetitionRules.every(Duration.standardMinutes(30))
            );
            scheduler.schedule(
                    nitroScheduleUpdateTask(
                            0,
                            0,
                            nitroTodayThreadCount,
                            nitroTodayRateLimit,
                            Optional.of(Predicates.alwaysTrue()),
                            "Nitro full fetch today updater",
                            "nitro.fullFetchTodayUpdater"
                    ),
                    RepetitionRules.NEVER
            );
            scheduler.schedule(
                    nitroScheduleUpdateTask(
                            30,
                            -8,
                            nitroThreeWeekThreadCount,
                            nitroThreeWeekRateLimit,
                            Optional.of(Predicates.alwaysTrue()),
                            "Nitro full fetch -8 to -30 day updater",
                            "nitro.fullFetch-8To-30DayUpdater."),
                    RepetitionRules.every(Duration.standardHours(12))
            );
            scheduler.schedule(
                    nitroScheduleUpdateTask(
                            7,
                            3,
                            nitroAroundTodayThreadCount,
                            nitroAroundTodayRateLimit,
                            Optional.of(Predicates.alwaysTrue()),
                            "Nitro full fetch -7 to +3 day updater",
                            "nitro.fullFetch-7To+3DayUpdater."
                    ),
                    RepetitionRules.every(Duration.standardHours(2))
            );
        }
        if (offScheduleIngestEnabled) {
            scheduler.schedule(
                    nitroOffScheduleIngestTask().withName("Nitro off-schedule content updater"),
                    RepetitionRules.every(Duration.standardHours(3))
            );
        }
    }

    private ScheduledTask nitroScheduleUpdateTask(
            int back,
            int forward,
            Integer threadCount,
            Integer rateLimit,
            Optional<Predicate<Item>> fullFetchPermittedPredicate,
            String taskName,
            String metricPrefix
    ) {
        DayRangeChannelDaySupplier drcds = new DayRangeChannelDaySupplier(bbcChannelSupplier(), dayRangeSupplier(back, forward));

        ExecutorService executor = Executors.newFixedThreadPool(threadCount, nitroThreadFactory);
        return new ChannelDayProcessingTask(
                executor,
                drcds,
                nitroChannelDayProcessor(rateLimit, fullFetchPermittedPredicate),
                null,
                jobFailureThresholdPercent,
                metricRegistry,
                metricPrefix
        )
                .withName(taskName);
    }

    private ScheduledTask nitroOffScheduleIngestTask() {
        Glycerin glycerin = glycerin(null);
        return new OffScheduleContentIngestTask(
                nitroContentAdapter(glycerin),
                nitroRequestPageSize,
                contentWriter(),
                pidLock,
                localOrRemoteNitroFetcher(
                        glycerin,
                        Optional.of(Predicates.alwaysTrue()),
                        "nitro.offSchedule."
                ),
                metricRegistry,
                "nitro.offSchedule."
        );
    }

    private ScheduledTask channelIngestTask() {
        Glycerin glycerin = glycerin(null);
        return ChannelIngestTask.create(nitroChannelAdapter(glycerin), channelWriter, channelResolver, new NitroChannelHydrator());
    }

    public ContentWriter contentWriter() {
        return new LastUpdatedSettingContentWriter(contentResolver, contentWriter);
    }

    @Bean
    NitroForceUpdateController pidUpdateController() {
        Glycerin glycerin = glycerin(null);
        return new NitroForceUpdateController(
                nitroContentAdapter(glycerin),
                nitroChannelAdapter(glycerin),
                contentWriter(),
                channelWriter,
                localOrRemoteNitroFetcher(
                        glycerin,
                        Optional.of(Predicates.alwaysTrue()),
                        "nitro.pidUpdateController."
                )
        );
    }

    @Bean
    ScheduleDayUpdateController nitroScheduleUpdateController() {
        return new ScheduleDayUpdateController(
                channelResolver,
                nitroChannelDayProcessor(
                        nitroTodayRateLimit,
                        Optional.of(Predicates.alwaysTrue())
                )
        );
    }

    ChannelDayProcessor nitroChannelDayProcessor(
            Integer rateLimit,
            Optional<Predicate<Item>> fullFetchPermitted
    ) {
        ContentWriter contentWriter = contentWriter();
        ScheduleResolverBroadcastTrimmer scheduleTrimmer = new ScheduleResolverBroadcastTrimmer(
                Publisher.BBC_NITRO,
                scheduleResolver,
                contentResolver,
                contentWriter
        );
        Glycerin glycerin = glycerin(rateLimit);
        return new NitroScheduleDayUpdater(
                scheduleWriter,
                scheduleTrimmer,
                nitroBroadcastHandler(glycerin, fullFetchPermitted, contentWriter),
                glycerin,
                metricRegistry,
                "nitro.dayUpdater."
        );
    }

    Glycerin glycerin(Integer rateLimit) {
        if (!tasksEnabled) {
            return UnconfiguredGlycerin.get();
        }

        Builder glycerin = XmlGlycerin.builder(nitroApiKey).withRootResource(nitroRoot);
        if (rateLimit != null) {
            glycerin.withLimiter(RateLimiter.create(rateLimit));
        }
        return glycerin.build();
    }

    NitroBroadcastHandler<ImmutableList<Optional<ItemRefAndBroadcast>>> nitroBroadcastHandler(
            Glycerin glycerin,
            Optional<Predicate<Item>> fullFetchPermitted,
            ContentWriter contentWriter
    ) {
        return new ContentUpdatingNitroBroadcastHandler(
                contentResolver,
                contentWriter,
                localOrRemoteNitroFetcher(glycerin, fullFetchPermitted, "nitro.BroadcastHandler."),
                pidLock
        );
    }
    
    LocalOrRemoteNitroFetcher localOrRemoteNitroFetcher(
            Glycerin glycerin,
            Optional<Predicate<Item>> fullFetchPermitted,
            String metrixPrefix
    ) {
        if (fullFetchPermitted.isPresent()) {
            return new LocalOrRemoteNitroFetcher(contentResolver, nitroContentAdapter(glycerin), fullFetchPermitted.get(), metricRegistry, metrixPrefix + "localOrRemoteFetcher.");
        } else {
            return new LocalOrRemoteNitroFetcher(contentResolver, nitroContentAdapter(glycerin), new SystemClock(), metricRegistry, metrixPrefix + "localOrRemoteFetcher");
        }
    }
    
    

    GlycerinNitroContentAdapter nitroContentAdapter(Glycerin glycerin) {
        SystemClock clock = new SystemClock();
        GlycerinNitroClipsAdapter clipsAdapter = new GlycerinNitroClipsAdapter(
                glycerin,
                clock,
                nitroRequestPageSize,
                metricRegistry,
                "nitro.clipsAdapter."
        );

        return new GlycerinNitroContentAdapter(glycerin, clipsAdapter, peopleWriter, clock, nitroRequestPageSize, metricRegistry, "nitro.contentAdapter.");
    }

    GlycerinNitroChannelAdapter nitroChannelAdapter(Glycerin glycerin) {
        return GlycerinNitroChannelAdapter.create(glycerin);
    }

    private Supplier<Range<LocalDate>> dayRangeSupplier(int back, int forward) {
        return AroundTodayDayRangeSupplier.builder()
                .withDaysBack(back)
                .withDaysForward(forward)
                .build();
    }

    private Supplier<ImmutableSet<Channel>> bbcChannelSupplier() {
        return new Supplier<ImmutableSet<Channel>>() {
            @Override
            public ImmutableSet<Channel> get() {
                return ImmutableSet.copyOf(channelResolver.allChannels(ChannelQuery.builder()
                        .withPublisher(Publisher.BBC_NITRO)
                        .build()));
            }
        };
    }
}

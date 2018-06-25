package org.atlasapi.equiv.channel;

import com.google.api.client.util.Sets;
import com.metabroadcast.common.scheduling.RepetitionRule;
import com.metabroadcast.common.scheduling.RepetitionRules;
import com.metabroadcast.common.scheduling.ScheduledTask;
import com.metabroadcast.common.scheduling.SimpleScheduler;
import org.atlasapi.equiv.update.EquivalenceUpdater;
import org.atlasapi.equiv.update.www.ChannelEquivalenceUpdateController;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.entity.Publisher;
import org.joda.time.Duration;
import org.joda.time.LocalTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import javax.annotation.PostConstruct;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Configuration
@Import({ ChannelEquivModule.class })
public class ChannelEquivTaskModule {

    private static final Logger log = LoggerFactory.getLogger(ChannelEquivTaskModule.class);

    private static final int STARTUP_THREAD_COUNT = 2;

    private static final String TASK_NAME = " Channel Equiv Updater";

    private static final LocalTime BT_NON_PROD_BASE_START_TIME = new LocalTime(16, 30);

    private static final RepetitionRule BT_PROD_REPETITION = RepetitionRules.every(Duration.standardHours(2));
    private static final RepetitionRule BT_DEV_1_REPETITION = RepetitionRules.daily(BT_NON_PROD_BASE_START_TIME);
    private static final RepetitionRule BT_DEV_2_REPETITION = RepetitionRules.daily(BT_NON_PROD_BASE_START_TIME.plusHours(1));
    private static final RepetitionRule BT_REF_REPETITION = RepetitionRules.daily(BT_NON_PROD_BASE_START_TIME.plusHours(2));
    private static final RepetitionRule BARB_CHANNEL_REPETITION = RepetitionRules.NEVER;

    @Value("${equiv.updater.enabled}") private boolean updaterEnabled;
    @Value("${channel.equiv.enabled}") private boolean channelEquivEnabled;

    @Autowired private SimpleScheduler taskScheduler;
    @Autowired private EquivalenceUpdater<Channel> equivalenceUpdater;
    @Autowired private ChannelResolver channelResolver;

    @PostConstruct
    public void scheduleUpdater() {

        ExecutorService executorService = Executors.newFixedThreadPool(STARTUP_THREAD_COUNT);

        Set<ScheduledTask> jobsAtStartup = Sets.newHashSet();

        if (updaterEnabled && channelEquivEnabled) {
            log.info("Channel equivalence enabled");
            addEquivalenceJobs(jobsAtStartup);
        } else {
            log.debug("Channel equivalence disabled");
        }

        jobsAtStartup.forEach(executorService::submit);
    }

    @Bean
    public ChannelEquivalenceUpdateController channelEquivalenceUpdateController() {
        return ChannelEquivalenceUpdateController.create(equivalenceUpdater, channelResolver);
    }

    private void addEquivalenceJobs(Set<ScheduledTask> jobsAtStartup) {
        scheduleEquivalenceJob(
                createUpdateTask(Publisher.BT_TV_CHANNELS, equivalenceUpdater),
                BT_PROD_REPETITION,
                jobsAtStartup
        );
        scheduleEquivalenceJob(
                createUpdateTask(Publisher.BT_TV_CHANNELS_TEST1, equivalenceUpdater),
                BT_DEV_1_REPETITION,
                jobsAtStartup
        );
        scheduleEquivalenceJob(
                createUpdateTask(Publisher.BT_TV_CHANNELS_TEST2, equivalenceUpdater),
                BT_DEV_2_REPETITION,
                jobsAtStartup
        );
        scheduleEquivalenceJob(
                createUpdateTask(Publisher.BT_TV_CHANNELS_REFERENCE, equivalenceUpdater),
                BT_REF_REPETITION,
                jobsAtStartup
        );
        scheduleEquivalenceJob(
                createUpdateTask(Publisher.BARB_CHANNELS, equivalenceUpdater),
                BARB_CHANNEL_REPETITION,
                jobsAtStartup
        );
    }

    private Set<ScheduledTask> scheduleEquivalenceJob(
            ScheduledTask task,
            RepetitionRule repetitionRule,
            Set<ScheduledTask> jobsAtStartup
    ) {
        taskScheduler.schedule(task, repetitionRule);
        if (!RepetitionRules.NEVER.equals(repetitionRule)) {
            jobsAtStartup.add(task);
        }

        return jobsAtStartup;
    }

    private ScheduledTask createUpdateTask(
            Publisher publisher,
            EquivalenceUpdater<Channel> updater
    ) {
        return ChannelEquivalenceUpdateTask.create(channelResolver, publisher, updater)
                .withName(publisher.title() + TASK_NAME);
    }
}

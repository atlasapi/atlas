package org.atlasapi.equiv.channel;

import com.google.api.client.util.Sets;
import com.metabroadcast.common.scheduling.RepetitionRule;
import com.metabroadcast.common.scheduling.RepetitionRules;
import com.metabroadcast.common.scheduling.ScheduledTask;
import com.metabroadcast.common.scheduling.SimpleScheduler;
import org.atlasapi.equiv.channel.matchers.BtChannelMatcher;
import org.atlasapi.equiv.channel.matchers.ChannelMatcher;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.channel.ChannelWriter;
import org.atlasapi.media.entity.Publisher;
import org.joda.time.LocalTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import javax.annotation.PostConstruct;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Configuration
@Import({ChannelEquivModule.class})
public class ChannelEquivTaskModule {

    private static final Logger log = LoggerFactory.getLogger(ChannelEquivTaskModule.class);

    private static final String CHANNEL_EQUIV_TASK_NAME = " Channel Equiv Updater";

    private static final RepetitionRule BT_CHANNEL_REPETITION_RULE =
            RepetitionRules.daily(new LocalTime(6, 0));

    @Value("$equiv.updater.enabled") private String updaterEnabled;
    @Value("$channel.equiv.enabled") private String channelEquivEnabled;

    @Autowired private SimpleScheduler taskScheduler;
    @Autowired private BtChannelMatcher btChannelMatcher;
    @Autowired private ChannelResolver channelResolver;
    @Autowired private ChannelWriter channelWriter;


    @PostConstruct
    public void scheduleUpdater() {

        ExecutorService executorService = Executors.newFixedThreadPool(3);

        Set<ScheduledTask> jobsAtStartup = Sets.newHashSet();

        if (Boolean.parseBoolean(updaterEnabled) && Boolean.parseBoolean(channelEquivEnabled)) {
            addEquivalenceJobs(jobsAtStartup);
        }

        for (ScheduledTask scheduledTask : jobsAtStartup) {
            executorService.submit(scheduledTask);
        }
    }

    private void addEquivalenceJobs(Set<ScheduledTask> jobsAtStartup) {
        scheduleEquivalenceJob(
                createUpdateTask(Publisher.BT_TV_CHANNELS, btChannelMatcher),
                BT_CHANNEL_REPETITION_RULE,
                jobsAtStartup
        );
        scheduleEquivalenceJob(
                createUpdateTask(Publisher.BT_TV_CHANNELS_TEST1, btChannelMatcher),
                BT_CHANNEL_REPETITION_RULE,
                jobsAtStartup
        );
        scheduleEquivalenceJob(
                createUpdateTask(Publisher.BT_TV_CHANNELS_TEST2, btChannelMatcher),
                BT_CHANNEL_REPETITION_RULE,
                jobsAtStartup
        );
        scheduleEquivalenceJob(
                createUpdateTask(Publisher.BT_TV_CHANNELS_REFERENCE, btChannelMatcher),
                BT_CHANNEL_REPETITION_RULE,
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
            ChannelMatcher channelMatcher
    ) {
        return ChannelEquivalenceUpdateTask.builder(publisher.title() + CHANNEL_EQUIV_TASK_NAME)
                .forPublisher(publisher)
                .withChannelMatcher(channelMatcher)
                .withChannelResolver(channelResolver)
                .withChannelWriter(channelWriter)
                .build();
    }
}

package org.atlasapi.remotesite.metabroadcast.picks;

import java.util.concurrent.Executors;

import javax.annotation.PostConstruct;

import org.atlasapi.media.channel.ChannelGroupResolver;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.persistence.content.ContentGroupResolver;
import org.atlasapi.persistence.content.ContentGroupWriter;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ScheduleResolver;
import org.atlasapi.remotesite.bbc.nitro.ChannelDayProcessingTask;
import org.atlasapi.remotesite.bbc.nitro.DayRangeChannelDaySupplier;
import org.joda.time.LocalTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.scheduling.RepetitionRule;
import com.metabroadcast.common.scheduling.RepetitionRules;
import com.metabroadcast.common.scheduling.SimpleScheduler;

@Configuration
public class PicksModule {

    private static final RepetitionRule PICKS_SCHEDULE = RepetitionRules.daily(new LocalTime(6, 30, 0));
    
    @Value("${metabroadcast.picks.priorityChannelGroup}")
    private String priorityChannelGroupId;
    @Value("${updaters.metabroadcastpicks.enabled}")
    private Boolean updaterEnabled;
    
    @Autowired
    private ChannelGroupResolver channelGroupResolver;
    @Autowired
    private ChannelResolver channelResolver;
    @Autowired
    private ScheduleResolver scheduleResolver;
    @Autowired 
    private ContentGroupResolver contentGroupResolver;
    @Autowired 
    private ContentResolver contentResolver;
    @Autowired
    private ContentGroupWriter contentGroupWriter;
    @Autowired
    private DatabasedMongo mongo;
    @Autowired
    private SimpleScheduler scheduler;
    
    @Bean
    public ChannelDayProcessingTask picksScheduledTask() {
        return new ChannelDayProcessingTask(Executors.newSingleThreadExecutor(), 
                picksDayRangeChannelDaySupplier(), picksDayUpdater(), picksScheduledTaskListener());
    }
    
    @Bean
    public PicksScheduledTaskListener picksScheduledTaskListener() {
        return new PicksScheduledTaskListener(picksLastProcessedStore());
    }
    
    @Bean
    public DayRangeChannelDaySupplier picksDayRangeChannelDaySupplier() {
        return new DayRangeChannelDaySupplier(picksChannelsSupplier(), picksDayRangeSupplier());
    }
    
    @Bean
    public PicksChannelsSupplier picksChannelsSupplier() {
        return new PicksChannelsSupplier(channelGroupResolver, channelResolver, 
                priorityChannelGroupId);
    }
    
    @Bean
    public PicksDayUpdater picksDayUpdater() {
        return new PicksDayUpdater(scheduleResolver, contentGroupResolver, contentResolver, 
                contentGroupWriter, picksChannelsSupplier());
    }
    
    @Bean
    public PicksDayRangeSupplier picksDayRangeSupplier() {
        return new PicksDayRangeSupplier(picksLastProcessedStore());
    }
    @Bean
    public PicksLastProcessedStore picksLastProcessedStore() {
        return new MongoPicksLastProcessedStore(mongo);
    }
    
    @Bean
    public PicksChannelDayUpdateController picksChannelDayUpdateController() {
        return new PicksChannelDayUpdateController(channelResolver, picksDayUpdater());
    }
    
    @PostConstruct
    private void scheduleTask() {
        if (Boolean.TRUE.equals(updaterEnabled)) {
            scheduler.schedule(picksScheduledTask().withName("MetaBroadcast Picks Content Group Updater"), PICKS_SCHEDULE);
        }
    }
}

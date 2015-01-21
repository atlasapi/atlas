package org.atlasapi.remotesite.youview;

import javax.annotation.PostConstruct;

import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.ScheduleResolver;
import org.atlasapi.persistence.content.schedule.mongo.ScheduleWriter;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;
import org.atlasapi.remotesite.pa.channels.PaChannelsIngester;
import org.joda.time.Duration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.google.common.collect.ImmutableMap;
import com.metabroadcast.common.scheduling.SimpleScheduler;

@Configuration
@Import( YouViewCoreModule.class )
public class YouViewModule {

    private static final String YOUVIEW_PRODUCTION_ALIAS_PREFIX = "youview";
    private static final String YOUVIEW_STAGE_ALIAS_PREFIX = "youview_stage";
    
    private @Autowired SimpleScheduler scheduler;
    private @Autowired ChannelResolver channelResolver;
    private @Autowired ContentResolver contentResolver;
    private @Autowired ContentWriter contentWriter;
    private @Autowired ScheduleWriter scheduleWriter;
    private @Autowired ScheduleResolver scheduleResolver;
    private @Autowired YouViewChannelResolver youviewChannelResolver;
    private @Autowired LookupEntryStore lookupEntryStore;
    
    private @Value("${youview.prod.url}") String youViewProductionUri;
    private @Value("${youview.stage.url}") String youViewStageUri;
    private @Value("${youview.timeout.seconds}") int timeout;
    
    @Bean
    public YouViewEnvironmentIngester youViewProductionIngester() {
        return new YouViewEnvironmentIngester(youViewProductionUri, 
                Duration.standardSeconds(timeout), scheduler, 
                channelResolver, contentResolver, contentWriter, 
                scheduleWriter, scheduleResolver, youviewChannelResolver, 
                lookupEntryStore, productionConfiguration());
    }
    
    @Bean
    public YouViewEnvironmentIngester youViewStageIngester() {
        return new YouViewEnvironmentIngester(youViewStageUri, 
                Duration.standardSeconds(timeout), scheduler, 
                channelResolver, contentResolver, contentWriter, 
                scheduleWriter, scheduleResolver, youviewChannelResolver,
                lookupEntryStore, stageConfiguration());
    }
    
    @PostConstruct
    public void scheduleTasks() {
        youViewProductionIngester().startBackgroundTasks();
        youViewStageIngester().startBackgroundTasks();
    }
    
    private YouViewIngestConfiguration productionConfiguration() {
        return new YouViewIngestConfiguration(
                ImmutableMap.of(PaChannelsIngester.YOUVIEW_SERVICE_ID_ALIAS_PREFIX, Publisher.YOUVIEW,
                        YouViewCoreModule.SCOTLAND_SERVICE_ALIAS_PREFIX, Publisher.YOUVIEW_SCOTLAND_RADIO,
                        PaChannelsIngester.BT_SERVICE_ID_ALIAS_PREFIX, Publisher.YOUVIEW_BT),
                YOUVIEW_PRODUCTION_ALIAS_PREFIX);
    }
    
    private YouViewIngestConfiguration stageConfiguration() {
        return new YouViewIngestConfiguration(
                ImmutableMap.of(PaChannelsIngester.YOUVIEW_SERVICE_ID_ALIAS_PREFIX, Publisher.YOUVIEW_STAGE,
                        YouViewCoreModule.SCOTLAND_SERVICE_ALIAS_PREFIX, Publisher.YOUVIEW_SCOTLAND_RADIO_STAGE,
                        PaChannelsIngester.BT_SERVICE_ID_ALIAS_PREFIX, Publisher.YOUVIEW_BT_STAGE),
                YOUVIEW_STAGE_ALIAS_PREFIX);
    }
    
}

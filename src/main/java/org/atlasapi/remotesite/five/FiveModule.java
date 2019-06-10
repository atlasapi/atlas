package org.atlasapi.remotesite.five;

import javax.annotation.PostConstruct;

import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;

import com.metabroadcast.common.properties.Configurer;
import com.metabroadcast.common.scheduling.RepetitionRule;
import com.metabroadcast.common.scheduling.RepetitionRules;
import com.metabroadcast.common.scheduling.RepetitionRules.Daily;
import com.metabroadcast.common.scheduling.SimpleScheduler;

import org.joda.time.LocalTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@SuppressWarnings("PublicConstructor")
@Configuration
public class FiveModule {
    
    private static final Logger log = LoggerFactory.getLogger(FiveUpdater.class);

    // This ingester is known to take 17hrs to finish so we starting it in the evening to
    // take advantage of the night and have the data ready by mid-next day
    private final static Daily DAILY = RepetitionRules.daily(new LocalTime(20, 0, 0));
    
    private @Autowired SimpleScheduler scheduler;
    private @Autowired ContentWriter contentWriter;
    private @Autowired ContentResolver contentResolver;
    private @Autowired ChannelResolver channelResolver;

    private @Value("${service.web.id}") Long webServiceId;
    private @Value("${player.demand5.id}") Long demand5PlayerId;
    private @Value("${service.ios.id}") Long iOsServiceId;
    
    @PostConstruct
    public void startBackgroundTasks() {
        scheduler.schedule(fiveUpdater().withName("Five Updater"), RepetitionRules.NEVER);
        log.info("Installed Five updater");
    }
    
    @Bean
    public FiveUpdater fiveUpdater() {
        return FiveUpdater.create(
                contentWriter,
                channelResolver,
                contentResolver,
                fiveLocationPolicyIds(),
                Configurer.get("five.timeout.socket", "180").toInt()
        );
    }
    
    @Bean
    public FiveBrandUpdateController fiveBrandUpdateController() {
        return new FiveBrandUpdateController(fiveUpdater());
    }
    
    @Bean
    public FiveLocationPolicyIds fiveLocationPolicyIds() {
        return FiveLocationPolicyIds.builder()
                    .withDemand5PlayerId(demand5PlayerId)
                    .withIosServiceId(iOsServiceId)
                    .withWebServiceId(webServiceId)
                    .build();
    }
}

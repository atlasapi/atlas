package org.atlasapi.remotesite.itv.whatson;

import java.util.List;

import javax.annotation.PostConstruct;

import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.logging.AdapterLog;
import org.atlasapi.persistence.logging.AdapterLogEntry;
import org.atlasapi.persistence.logging.AdapterLogEntry.Severity;
import org.atlasapi.persistence.system.RemoteSiteClient;
import org.joda.time.Duration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.metabroadcast.common.http.HttpClients;
import com.metabroadcast.common.http.SimpleHttpClient;
import com.metabroadcast.common.scheduling.RepetitionRules;
import com.metabroadcast.common.scheduling.RepetitionRules.Every;
import com.metabroadcast.common.scheduling.SimpleScheduler;

@Configuration
public class ItvWhatsOnModule {
    
    private @Autowired SimpleScheduler scheduler;
    private @Autowired @Qualifier("contentResolver") ContentResolver contentResolver;
    private @Autowired @Qualifier("contentWriter") ContentWriter contentWriter;
    private @Autowired ChannelResolver channelResolver;
    private @Value("${itv.whatson.schedule.url}") String feedUrl;
    private @Value("${itv.whatson.percentageFailureToTriggerJobFailure}") int percentageFailureToTriggerJobFailure;
    
    private @Value("${service.web.id}") Long webServiceId;
    private @Value("${player.itvplayer.id}") Long itvPlayerId;

    private static final Every EVERY_FIFTEEN_MINUTES = RepetitionRules.every(Duration.standardMinutes(15));
    private static final Every EVERY_HOUR = RepetitionRules.every(Duration.standardHours(1));
    
    private @Autowired AdapterLog log;

    // Disabled since we have lost access to the source, and have good reason to believe that none
    // of the applications have access to this publisher anyway
    @PostConstruct
    public void startBackgroundTasks() {
        scheduler.schedule(itvWhatsOnUpdaterDaily().withName("ITV What's On Daily"), RepetitionRules.NEVER);
        scheduler.schedule(itvWhatsOnUpdaterPlusMinus7Day().withName("ITV What's On +/- 7 days"), RepetitionRules.NEVER);
        log.record(new AdapterLogEntry(Severity.INFO).withDescription("ITV What's On Schedule task installed.").withSource(getClass()));
    }
    
    @Bean
    public SimpleHttpClient httpClient() {
        return HttpClients.webserviceClient();
    }
    
    @Bean
    public RemoteSiteClient<List<ItvWhatsOnEntry>> itvWhatsOnClient() {
        return new ItvWhatsOnClient(httpClient());
    }
    
    @Bean
    public ItvWhatsOnEntryProcessor processor() {
        return new ItvWhatsOnEntryProcessor(contentResolver, contentWriter, 
                channelResolver, itvWhatsOnLocationPolicyIds());
    }
    
    @Bean
    public ItvWhatsOnUpdater itvWhatsOnUpdaterDaily() {
        return ItvWhatsOnUpdater.builder()
                .withFeedUrl(feedUrl)
                .withWhatsOnClient(itvWhatsOnClient())
                .withProcessor(processor())
                .withLookBack(0)
                .withLookAhead(0)
                .withPercentageFailureToTriggerJobFailure(percentageFailureToTriggerJobFailure)
                .build();
    }
    
    @Bean
    public ItvWhatsOnUpdater itvWhatsOnUpdaterPlusMinus7Day() {
        return ItvWhatsOnUpdater.builder()
                .withFeedUrl(feedUrl)
                .withWhatsOnClient(itvWhatsOnClient())
                .withProcessor(processor())
                .withLookBack(7)
                .withLookAhead(7)
                .withPercentageFailureToTriggerJobFailure(percentageFailureToTriggerJobFailure)
                .build();
    }

    @Bean
    public ItvWhatsOnController itvWhatsOnController() {
        return new ItvWhatsOnController(feedUrl, itvWhatsOnClient(), processor());
    }
    
    @Bean
    public ItvWhatsOnLocationPolicyIds itvWhatsOnLocationPolicyIds() {
        return ItvWhatsOnLocationPolicyIds
                    .builder()
                    .withItvPlayerId(itvPlayerId)
                    .withWebServiceId(webServiceId)
                    .build();
    }
}

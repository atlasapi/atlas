package org.atlasapi.remotesite.amazonunbox;

import javax.annotation.PostConstruct;

import org.atlasapi.media.entity.Content;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.listing.ContentLister;
import org.atlasapi.remotesite.ContentExtractor;
import org.atlasapi.remotesite.HttpClients;
import org.joda.time.LocalTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.metabroadcast.common.scheduling.RepetitionRules;
import com.metabroadcast.common.scheduling.RepetitionRules.Daily;
import com.metabroadcast.common.scheduling.SimpleScheduler;

@Configuration
public class AmazonUnboxModule {

    private final static Daily DAILY = RepetitionRules.daily(new LocalTime(4, 30, 0));
    
    private @Autowired SimpleScheduler scheduler;
    private @Autowired ContentWriter contentWriter;
    private @Autowired ContentLister contentLister;
    private @Autowired ContentResolver resolver;
    
    private @Value("${s3.access}") String s3access;
    private @Value("${s3.secret}") String s3secret;
    private @Value("${unbox.url}") String unboxUrl;
    private @Value("${unbox.missingContent.percentage}") Integer missingContentPercentage;
    
    @PostConstruct
    public void startBackgroundTasks() {
        scheduler.schedule(amazonUnboxUpdater().withName("Amazon Unbox Daily Updater"), DAILY);
    }

    @Bean
    public AmazonUnboxUpdateTask amazonUnboxUpdater() {
        
        AmazonUnboxPreProcessingItemProcessor preProcessor = new AmazonUnboxPreProcessingItemProcessor();
        
        ContentExtractor<AmazonUnboxItem,Iterable<Content>> contentExtractor = new AmazonUnboxContentExtractor();
        AmazonUnboxItemProcessor processor = new AmazonUnboxContentWritingItemProcessor(contentExtractor, resolver, contentWriter, contentLister, missingContentPercentage, preProcessor);
        
        return new AmazonUnboxUpdateTask(preProcessor, processor, feedSupplier());
    }
    
    @Bean
    public AmazonUnboxHttpFeedSupplier feedSupplier() {
        return new AmazonUnboxHttpFeedSupplier(HttpClients.webserviceClient(), unboxUrl);
    }
}

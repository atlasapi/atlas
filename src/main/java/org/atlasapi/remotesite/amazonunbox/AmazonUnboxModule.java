package org.atlasapi.remotesite.amazonunbox;

import javax.annotation.PostConstruct;

import org.atlasapi.media.entity.Content;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.listing.ContentLister;
import org.atlasapi.persistence.content.mongo.MongoContentWriter;
import org.atlasapi.persistence.media.entity.ContentTranslator;
import org.atlasapi.persistence.media.entity.DescribedTranslator;
import org.atlasapi.remotesite.ContentExtractor;

import com.metabroadcast.common.scheduling.RepetitionRules;
import com.metabroadcast.common.scheduling.RepetitionRules.Daily;
import com.metabroadcast.common.scheduling.SimpleScheduler;

import com.google.common.collect.ImmutableSet;
import org.joda.time.LocalTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class AmazonUnboxModule {

    private final static Daily DAILY = RepetitionRules.daily(new LocalTime(23, 0, 0));
    
    private @Autowired SimpleScheduler scheduler;
    private @Autowired ContentWriter contentWriter;
    private @Autowired ContentLister contentLister;
    private @Autowired ContentResolver contentResolver;

    private @Value("${unbox.url}") String unboxUrl;
    private @Value("${unbox.missingContent.percentage}") Integer missingContentPercentage;

    /**
     * These keys will be removed from the database if their values are empty during an ingest. Keys
     * not here will be retained but with empty values.
     * Maybe everything should be here, but who knows what everything really is!?
     */
    private static final Iterable<String> KEYS_TO_REMOVE = ImmutableSet.of(
            DescribedTranslator.IMAGE_KEY,
            DescribedTranslator.GENRES_KEY,
            ContentTranslator.YEAR_KEY,
            ContentTranslator.CERTIFICATES_KEY,
            "restriction" //hardcoded in the translator classes as well
    );
    
    @PostConstruct
    public void startBackgroundTasks() {
        scheduler.schedule(amazonUnboxUpdater().withName("Amazon Unbox Daily Updater"), DAILY);
    }

    @Bean
    public AmazonUnboxUpdateTask amazonUnboxUpdater() {
        
        AmazonUnboxPreProcessingItemProcessor preProcessor =
                new AmazonUnboxPreProcessingItemProcessor();
        
        ContentExtractor<AmazonUnboxItem,Iterable<Content>> contentExtractor =
                new AmazonUnboxContentExtractor();
        AmazonUnboxItemProcessor processor = new AmazonUnboxContentWritingItemProcessor(
                contentExtractor,
                contentResolver,
                contentWriter(),
                contentLister,
                missingContentPercentage,
                preProcessor
        );
        
        return new AmazonUnboxUpdateTask(preProcessor, processor, amazonUnboxFeedSupplier());
    }
    
    @Bean
    public AmazonUnboxHttpFeedSupplier amazonUnboxFeedSupplier() {
        return new AmazonUnboxHttpFeedSupplier(unboxUrl);
    }


    public ContentWriter contentWriter() {
        ContentWriter tmpContentWriter;
        if(contentWriter instanceof MongoContentWriter){
            tmpContentWriter = ((MongoContentWriter) contentWriter).withKeysToRemove(KEYS_TO_REMOVE);
        }
        else{
            tmpContentWriter = contentWriter;
        }
        return new LastUpdatedSettingContentWriter(contentResolver, tmpContentWriter);
    }
}

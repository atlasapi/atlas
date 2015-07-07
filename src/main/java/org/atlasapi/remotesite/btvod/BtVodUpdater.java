package org.atlasapi.remotesite.btvod;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.Set;

import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Topic;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.topic.TopicCreatingTopicResolver;
import org.atlasapi.persistence.topic.TopicWriter;
import org.atlasapi.remotesite.btvod.contentgroups.BtVodContentGroupUpdater;
import org.atlasapi.remotesite.btvod.topics.BtVodStaleTopicContentRemover;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.metabroadcast.common.scheduling.ScheduledTask;


public class BtVodUpdater extends ScheduledTask {

    private static final Logger log = LoggerFactory.getLogger(BtVodUpdater.class);
    
    private final ContentResolver resolver;
    private final ContentWriter writer;
    private final String uriPrefix;
    private final Publisher publisher;
    private final BtVodData vodData;
    private final BtVodContentGroupUpdater contentGroupUpdater;
    private final BtVodOldContentDeactivator oldContentDeactivator;
    private final String baseUrl;
    private final ImageExtractor imageExtractor;
    private final BrandImageExtractor brandImageExtractor;
    private final BrandUriExtractor brandUriExtractor;
    private final TopicCreatingTopicResolver topicResolver;
    private final TopicWriter topicWriter;
    private final BtVodContentMatchingPredicate newFeedContentMatchingPredicate;
    private final Topic newTopic;
    private final BtVodStaleTopicContentRemover staleTopicContentRemover;
    
    public BtVodUpdater(ContentResolver resolver, ContentWriter contentWriter,
            BtVodData vodData, String uriPrefix,
            BtVodContentGroupUpdater contentGroupUpdater,
            Publisher publisher, BtVodOldContentDeactivator oldContentDeactivator,
            BrandImageExtractor brandImageExtractor, String baseUrl, ImageExtractor imageExtractor,
            BrandUriExtractor brandUriExtractor, TopicCreatingTopicResolver topicResolver,
            TopicWriter topicWriter, BtVodContentMatchingPredicate newFeedContentMatchingPredicate,
            Topic newTopic, BtVodStaleTopicContentRemover staleTopicContentRemover) {
        this.staleTopicContentRemover = staleTopicContentRemover;
        this.topicResolver = checkNotNull(topicResolver);
        this.topicWriter = checkNotNull(topicWriter);
        this.newFeedContentMatchingPredicate = checkNotNull(newFeedContentMatchingPredicate);
        this.newTopic = checkNotNull(newTopic);
        this.resolver = checkNotNull(resolver);
        this.writer = checkNotNull(contentWriter);
        this.vodData = checkNotNull(vodData);
        this.uriPrefix = checkNotNull(uriPrefix);
        this.publisher = checkNotNull(publisher);
        this.contentGroupUpdater = checkNotNull(contentGroupUpdater);
        this.oldContentDeactivator = checkNotNull(oldContentDeactivator);
        this.brandImageExtractor = checkNotNull(brandImageExtractor);
        this.brandUriExtractor = checkNotNull(brandUriExtractor);
        this.baseUrl = checkNotNull(baseUrl);
        this.imageExtractor = checkNotNull(imageExtractor);
        
        withName("BT VOD Catalogue Ingest");
    }

    @Override
    public void runTask() {
        
        newFeedContentMatchingPredicate.init();
        
        BtVodDescribedFieldsExtractor describedFieldsExtractor = new BtVodDescribedFieldsExtractor(topicResolver, topicWriter, publisher, newFeedContentMatchingPredicate, newTopic);
        
        brandImageExtractor.start();
        
        MultiplexingVodContentListener listeners 
            = new MultiplexingVodContentListener(
                    ImmutableList.of(oldContentDeactivator, contentGroupUpdater, staleTopicContentRemover));
        Set<String> processedRows = Sets.newHashSet();
        
        listeners.beforeContent();
        
        BtVodBrandWriter brandExtractor = new BtVodBrandWriter(
                writer,
                resolver,
                publisher,
                uriPrefix,
                listeners,
                processedRows,
                new TitleSanitiser(), 
                describedFieldsExtractor,
                brandImageExtractor,
                brandUriExtractor
        );
        BtVodSeriesWriter seriesExtractor = new BtVodSeriesWriter(
                writer,
                resolver,
                brandExtractor,
                publisher,
                listeners,
                describedFieldsExtractor, 
                processedRows
        );
        BtVodItemWriter itemExtractor = new BtVodItemWriter(
                writer,
                resolver,
                brandExtractor,
                seriesExtractor,
                publisher,
                uriPrefix,
                listeners,
                describedFieldsExtractor,
                processedRows,
                new BtVodPricingAvailabilityGrouper(),
                new TitleSanitiser(),
                imageExtractor
        );
        
        try {
            reportStatus("Extracting brand images");
            vodData.processData(brandImageExtractor);
            
            reportStatus("Brand extract [IN PROGRESS]  Series extract [TODO]  Item extract [TODO]");
            vodData.processData(brandExtractor);
            reportStatus(String.format("Brand extract [DONE: %d rows successful %d rows failed]  Series extract [IN PROGRESS]  Item extract [TODO]  Content group update [TODO]", 
                    brandExtractor.getResult().getProcessed(), 
                    brandExtractor.getResult().getFailures()));

            vodData.processData(seriesExtractor);
            reportStatus(String.format("Brand extract [DONE: %d rows successful %d rows failed]  Series extract [DONE: %d rows successful %d rows failed]  Item extract [IN PROGRESS]  Content group update [TODO]", 
                    brandExtractor.getResult().getProcessed(), 
                    brandExtractor.getResult().getFailures(), 
                    seriesExtractor.getResult().getProcessed(), 
                    seriesExtractor.getResult().getFailures()));

            vodData.processData(itemExtractor);
            reportStatus(String.format("Brand extract [DONE: %d rows successful %d rows failed]  Series extract [DONE: %d rows successful %d rows failed]  Item extract [DONE: %d rows successful %d rows failed] Content group update [IN PROGRESS]",
                    brandExtractor.getResult().getProcessed(), 
                    brandExtractor.getResult().getFailures(), 
                    seriesExtractor.getResult().getProcessed(), 
                    seriesExtractor.getResult().getFailures(),
                    itemExtractor.getResult().getProcessed(),
                    itemExtractor.getResult().getFailures()));
            
            
            listeners.afterContent();
            
            if (brandExtractor.getResult().getFailures() > 0
                    || seriesExtractor.getResult().getFailures() > 0
                    || itemExtractor.getResult().getFailures() > 0) {
                throw new RuntimeException("Failed to extract some rows");
            }
        } catch (IOException e) {
            log.error("Extraction failed", e);
            Throwables.propagate(e);
        }
        
    }
    
    
}

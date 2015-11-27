package org.atlasapi.remotesite.btvod;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Series;
import org.atlasapi.media.entity.Topic;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.topic.TopicQueryResolver;
import org.atlasapi.remotesite.btvod.contentgroups.BtVodContentGroupUpdater;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.metabroadcast.common.scheduling.ScheduledTask;


public class BtVodUpdater extends ScheduledTask {

    private static final Logger log = LoggerFactory.getLogger(BtVodUpdater.class);

    private final ContentWriter contentWriter;
    private final String uriPrefix;
    private final Publisher publisher;
    private final BtVodData vodData;
    private final BtVodContentGroupUpdater contentGroupUpdater;
    private final BtVodOldContentDeactivator oldContentDeactivator;
    private final ImageExtractor imageExtractor;
    private final BrandImageExtractor brandImageExtractor;
    private final BrandUriExtractor brandUriExtractor;
    private final BtVodContentMatchingPredicate newFeedContentMatchingPredicate;
    private final Set<Topic> topicsToPropagateToParent;
    private final Set<String> topicNamespacesToPropagateToParent;
    private final BtVodSeriesUriExtractor seriesUriExtractor;
    private final BtVodVersionsExtractor versionsExtractor;
    private final BtVodDescribedFieldsExtractor describedFieldsExtractor;
    private final BtMpxVodClient mpxClient;
    private final TopicQueryResolver topicQueryResolver;
    
    public BtVodUpdater(
            ContentWriter contentWriter,
            BtVodData vodData,
            String uriPrefix,
            BtVodContentGroupUpdater contentGroupUpdater,
            Publisher publisher,
            BtVodOldContentDeactivator oldContentDeactivator,
            BrandImageExtractor brandImageExtractor,
            ImageExtractor imageExtractor,
            BrandUriExtractor brandUriExtractor,
            BtVodContentMatchingPredicate newFeedContentMatchingPredicate,
            Set<Topic> topicsToPropagateToParent,
            Set<String> topicNamespacesToPropagateToParent,
            BtVodSeriesUriExtractor seriesUriExtractor,
            BtVodVersionsExtractor versionsExtractor,
            BtVodDescribedFieldsExtractor describedFieldsExtractor,
            BtMpxVodClient mpxClient,
            TopicQueryResolver topicQueryResolver
    ) {
        this.contentWriter = checkNotNull(contentWriter);
        this.newFeedContentMatchingPredicate = checkNotNull(newFeedContentMatchingPredicate);
        this.topicsToPropagateToParent = checkNotNull(topicsToPropagateToParent);
        this.topicNamespacesToPropagateToParent = checkNotNull(topicNamespacesToPropagateToParent);
        this.vodData = checkNotNull(vodData);
        this.uriPrefix = checkNotNull(uriPrefix);
        this.publisher = checkNotNull(publisher);
        this.contentGroupUpdater = checkNotNull(contentGroupUpdater);
        this.oldContentDeactivator = checkNotNull(oldContentDeactivator);
        this.brandImageExtractor = checkNotNull(brandImageExtractor);
        this.brandUriExtractor = checkNotNull(brandUriExtractor);
        this.imageExtractor = checkNotNull(imageExtractor);
        this.seriesUriExtractor = checkNotNull(seriesUriExtractor);
        this.versionsExtractor = checkNotNull(versionsExtractor);
        this.describedFieldsExtractor = checkNotNull(describedFieldsExtractor);
        this.mpxClient = checkNotNull(mpxClient);
        this.topicQueryResolver = checkNotNull(topicQueryResolver);
    }

    @Override
    public void runTask() {
        
        newFeedContentMatchingPredicate.init();
        describedFieldsExtractor.init();
        
        brandImageExtractor.start();
        
        MultiplexingVodContentListener listeners 
            = new MultiplexingVodContentListener(
                ImmutableList.<BtVodContentListener>of(contentGroupUpdater)
        );
        Set<String> processedRows = Sets.newHashSet();
        
        listeners.beforeContent();

        TopicUpdater topicUpdater = new TopicUpdater(
                topicQueryResolver,
                topicsToPropagateToParent,
                topicNamespacesToPropagateToParent
        );
        
        BtVodBrandExtractor brandExtractor = new BtVodBrandExtractor(
                publisher,
                listeners,
                processedRows,
                describedFieldsExtractor,
                brandImageExtractor,
                brandUriExtractor
        );

        String brandExtractStatus = "[TODO]";
        String explicitSeriesExtractStatus = "[TODO]";
        String synthesizedSeriesExtractStatus = "[TODO]";
        String itemExtractStatus = "[TODO]";

        try {
            reportStatus("Extracting brand images");
            vodData.processData(brandImageExtractor);
            brandExtractStatus = "[IN PROGRESS]";
            reportStatus(
                    String.format(
                            "Brand extract %s, Explicit series extract %s, Synthesized series extract %s, Item extract %s",
                            brandExtractStatus,
                            explicitSeriesExtractStatus,
                            synthesizedSeriesExtractStatus,
                            itemExtractStatus
                    )
            );
            vodData.processData(brandExtractor);
            brandExtractStatus = String.format(
                "[DONE: %d rows successful, %d rows failed, %d brands extracted]",
                    brandExtractor.getResult().getProcessed(),
                    brandExtractor.getResult().getFailures(),
                    brandExtractor.getProcessedBrands().size()
            );
            explicitSeriesExtractStatus = "[IN PROGRESS]";
            reportStatus(
                    String.format(
                            "Brand extract %s, Explicit series extract %s, Synthesized series extract %s, Item extract %s",
                            brandExtractStatus,
                            explicitSeriesExtractStatus,
                            synthesizedSeriesExtractStatus,
                            itemExtractStatus
                    )
            );
            
            BtVodBrandProvider brandProvider = new BtVodBrandProvider(
                    brandUriExtractor,
                    brandExtractor.getProcessedBrands(),
                    new DescriptionAndImageUpdater(),
                    new CertificateUpdater(),
                    topicUpdater,
                    listeners
            );
            
            BtVodExplicitSeriesExtractor explicitSeriesExtractor = new BtVodExplicitSeriesExtractor(
                    brandProvider,
                    publisher,
                    listeners,
                    describedFieldsExtractor,
                    processedRows,
                    seriesUriExtractor,
                    versionsExtractor,
                    new TitleSanitiser(),
                    imageExtractor
            );
            
            vodData.processData(explicitSeriesExtractor);
            explicitSeriesExtractStatus = String.format(
                    "[DONE: %d rows successful, %d rows failed, %d series extracted]",
                    explicitSeriesExtractor.getResult().getProcessed(),
                    explicitSeriesExtractor.getResult().getFailures(),
                    explicitSeriesExtractor.getExplicitSeries().size()
            );
            synthesizedSeriesExtractStatus = "[IN PROGRESS]";
            reportStatus(
                    String.format(
                            "Brand extract %s, Explicit series extract %s, Synthesized series extract %s, Item extract %s",
                            brandExtractStatus,
                            explicitSeriesExtractStatus,
                            synthesizedSeriesExtractStatus,
                            itemExtractStatus
                    )
            );


            Map<String, Series> explicitSeries = explicitSeriesExtractor.getExplicitSeries();

            BtVodSynthesizedSeriesExtractor synthesizedSeriesExtractor = new BtVodSynthesizedSeriesExtractor(
                    brandProvider,
                    publisher,
                    listeners,
                    describedFieldsExtractor,
                    processedRows,
                    seriesUriExtractor,
                    explicitSeries.keySet(),
                    imageExtractor
            );
            vodData.processData(synthesizedSeriesExtractor);
            synthesizedSeriesExtractStatus = String.format(
                    "[DONE: %d rows successful, %d rows failed, %d series extracted]",
                    synthesizedSeriesExtractor.getResult().getProcessed(),
                    synthesizedSeriesExtractor.getResult().getFailures(),
                    synthesizedSeriesExtractor.getSynthesizedSeries().size()
            );
            itemExtractStatus = "[IN PROGRESS]";
            reportStatus(
                    String.format(
                            "Brand extract %s, Explicit series extract %s, Synthesized series extract %s, Item extract %s",
                            brandExtractStatus,
                            explicitSeriesExtractStatus,
                            synthesizedSeriesExtractStatus,
                            itemExtractStatus
                    )
            );

            Map<String, Series> synthesizedSeries = synthesizedSeriesExtractor.getSynthesizedSeries();

            BtVodSeriesProvider seriesProvider = new BtVodSeriesProvider(
                    explicitSeries,
                    synthesizedSeries,
                    seriesUriExtractor,
                    new DescriptionAndImageUpdater(),
                    new CertificateUpdater(),
                    brandProvider,
                    topicUpdater,
                    listeners
            );

            BtVodItemExtractor itemExtractor = new BtVodItemExtractor(
                    brandProvider,
                    seriesProvider,
                    publisher,
                    uriPrefix,
                    listeners,
                    describedFieldsExtractor,
                    processedRows,
                    new TitleSanitiser(),
                    imageExtractor,
                    versionsExtractor,
                    new BtVodMpxBackedEpisodeNumberExtractor(mpxClient),
                    mpxClient
            );

            vodData.processData(itemExtractor);
            itemExtractStatus = String.format(
                    "[DONE: %d rows successful, %d rows failed, %d items extracted]",
                    itemExtractor.getResult().getProcessed(),
                    itemExtractor.getResult().getFailures(),
                    itemExtractor.getProcessedItems().size()
            );
            reportStatus(
                    String.format(
                            "Brand extract %s, Explicit series extract %s, Synthesized series extract %s, Item extract %s, writing content... ",
                            brandExtractStatus,
                            explicitSeriesExtractStatus,
                            synthesizedSeriesExtractStatus,
                            itemExtractStatus
                    )
            );

            writeContent(
                    Iterables.concat(
                            brandProvider.getBrands(),
                            seriesProvider.getExplicitSeries(),
                            seriesProvider.getSynthesisedSeries(),
                            itemExtractor.getProcessedItems().values()
                    )
            );

            reportStatus(
                    String.format(
                            "Brand extract %s, Explicit series extract %s, Synthesized series extract %s, Item extract %s, content written. Content group update [IN PROGRESS]",
                            brandExtractStatus,
                            explicitSeriesExtractStatus,
                            synthesizedSeriesExtractStatus,
                            itemExtractStatus
                    )
            );

            listeners.afterContent();

            reportStatus(
                    String.format(
                            "Brand extract %s, Explicit series extract %s, Synthesized series extract %s, Item extract %s, content written. content group update [DONE]",
                            brandExtractStatus,
                            explicitSeriesExtractStatus,
                            synthesizedSeriesExtractStatus,
                            itemExtractStatus
                    )
            );

            if (brandExtractor.getResult().getFailures() > 0
                    || explicitSeriesExtractor.getResult().getFailures() > 0
                    || synthesizedSeriesExtractor.getResult().getFailures() > 0
                    || itemExtractor.getResult().getFailures() > 0) {
                throw new RuntimeException("Failed to extract some rows");
            }
        } catch (IOException e) {
            log.error("Extraction failed", e);
            throw Throwables.propagate(e);
        }
        
    }

    private void writeContent(Iterable<Content> contents) {
        oldContentDeactivator.beforeContent();
        for (Content content : contents) {
            if (content instanceof Container) {
                contentWriter.createOrUpdate((Container)content);

            }
            if (content instanceof Item) {
                contentWriter.createOrUpdate((Item)content);
            }
            oldContentDeactivator.onContent(content, null);
        }
        oldContentDeactivator.afterContent();
    }
}

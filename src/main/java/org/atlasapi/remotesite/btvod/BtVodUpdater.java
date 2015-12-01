package org.atlasapi.remotesite.btvod;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.Set;

import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
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
    private final BtVodEntryMatchingPredicate kidsPredicate;

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
            TopicQueryResolver topicQueryResolver,
            BtVodEntryMatchingPredicate kidsPredicate
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
        this.kidsPredicate = checkNotNull(kidsPredicate);
    }

    @Override
    public void runTask() {
        newFeedContentMatchingPredicate.init();
        describedFieldsExtractor.init();

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

        ProgressReporter reporter = new ProgressReporter();

        try {
            extractBrandImages(brandImageExtractor, reporter);

            BtVodBrandExtractor brandExtractor = extractBrands(
                    listeners, processedRows, reporter
            );
            BtVodBrandProvider brandProvider = getBrandProvider(
                    listeners, topicUpdater, brandExtractor
            );

            extractCollections(brandProvider, reporter);

            BtVodExplicitSeriesExtractor explicitSeriesExtractor = extractExplicitSeries(
                    brandProvider, listeners, processedRows, reporter
            );
            BtVodSynthesizedSeriesExtractor synthesizedSeriesExtractor = extractSynthesisedSeries(
                    listeners, processedRows, brandProvider, explicitSeriesExtractor, reporter
            );
            BtVodSeriesProvider seriesProvider = getSeriesProvider(
                    listeners, topicUpdater, brandProvider,
                    explicitSeriesExtractor, synthesizedSeriesExtractor
            );

            BtVodItemExtractor itemExtractor = extractItems(
                    listeners, processedRows, brandProvider, seriesProvider, reporter
            );

            writeContent(brandProvider, seriesProvider, itemExtractor, reporter);
            writeContentGroups(listeners, reporter);

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

    private void extractBrandImages(BrandImageExtractor brandImageExtractor,
            ProgressReporter reporter) throws IOException {
        reporter.updateBrandImagesExtractStatus(ProgressReporter.IN_PROGRESS);

        brandImageExtractor.start();
        vodData.processData(brandImageExtractor);

        reporter.updateBrandImagesExtractStatus(ProgressReporter.DONE);
    }

    private BtVodBrandExtractor extractBrands(MultiplexingVodContentListener listeners,
            Set<String> processedRows, ProgressReporter reporter) throws IOException {
        reporter.updateBrandExtractStatus(ProgressReporter.IN_PROGRESS);

        BtVodBrandExtractor brandExtractor = new BtVodBrandExtractor(
                publisher,
                listeners,
                processedRows,
                describedFieldsExtractor,
                brandImageExtractor,
                brandUriExtractor
        );

        vodData.processData(brandExtractor);

        reporter.updateBrandExtractStatus(String.format(
                "[DONE: %d rows successful, %d rows failed, %d brands extracted]",
                brandExtractor.getResult().getProcessed(),
                brandExtractor.getResult().getFailures(),
                brandExtractor.getProcessedBrands().size()
        ));

        return brandExtractor;
    }

    private BtVodBrandProvider getBrandProvider(MultiplexingVodContentListener listeners,
            TopicUpdater topicUpdater, BtVodBrandExtractor brandExtractor) {
        return new BtVodBrandProvider(
                brandUriExtractor,
                brandExtractor.getProcessedBrands(),
                brandExtractor.getParentGuidToBrand(),
                new HierarchyDescriptionAndImageUpdater(),
                new CertificateUpdater(),
                topicUpdater,
                listeners
        );
    }

    private void extractCollections(BtVodBrandProvider brandProvider, ProgressReporter reporter)
        throws IOException {
        reporter.updateCollectionExtractStatus(ProgressReporter.IN_PROGRESS);

        BtVodCollectionExtractor collectionExtractor = new BtVodCollectionExtractor(
                brandProvider, imageExtractor
        );

        vodData.processData(collectionExtractor);

        reporter.updateCollectionExtractStatus(String.format(
                "[DONE: %d rows successful, %d rows failed]",
                collectionExtractor.getResult().getProcessed(),
                collectionExtractor.getResult().getFailures()
        ));
    }

    private BtVodExplicitSeriesExtractor extractExplicitSeries(BtVodBrandProvider brandProvider,
            MultiplexingVodContentListener listeners, Set<String> processedRows,
            ProgressReporter reporter)
        throws IOException {
        reporter.updateExplicitSeriesExtractStatus(ProgressReporter.IN_PROGRESS);

        BtVodExplicitSeriesExtractor explicitSeriesExtractor = new BtVodExplicitSeriesExtractor(
                brandProvider,
                publisher,
                listeners,
                describedFieldsExtractor,
                processedRows,
                seriesUriExtractor,
                versionsExtractor,
                new TitleSanitiser(),
                imageExtractor,
                new DedupedDescriptionAndImageUpdater()
        );

        vodData.processData(explicitSeriesExtractor);

        reporter.updateExplicitSeriesExtractStatus(String.format(
                "[DONE: %d rows successful, %d rows failed, %d series extracted]",
                explicitSeriesExtractor.getResult().getProcessed(),
                explicitSeriesExtractor.getResult().getFailures(),
                explicitSeriesExtractor.getExplicitSeries().size()
        ));

        return explicitSeriesExtractor;
    }

    private BtVodSynthesizedSeriesExtractor extractSynthesisedSeries(
            MultiplexingVodContentListener listeners, Set<String> processedRows,
            BtVodBrandProvider brandProvider, BtVodExplicitSeriesExtractor explicitSeriesExtractor,
            ProgressReporter reporter) throws IOException {
        reporter.updateSynthesizedSeriesExtractStatus(ProgressReporter.IN_PROGRESS);

        BtVodSynthesizedSeriesExtractor synthesizedSeriesExtractor =
                new BtVodSynthesizedSeriesExtractor(
                    brandProvider,
                    publisher,
                    listeners,
                    describedFieldsExtractor,
                    processedRows,
                    seriesUriExtractor,
                    explicitSeriesExtractor.getExplicitSeries().keySet()
                );

        vodData.processData(synthesizedSeriesExtractor);

        reporter.updateSynthesizedSeriesExtractStatus(String.format(
                "[DONE: %d rows successful, %d rows failed, %d series extracted]",
                synthesizedSeriesExtractor.getResult().getProcessed(),
                synthesizedSeriesExtractor.getResult().getFailures(),
                synthesizedSeriesExtractor.getSynthesizedSeries().size()
        ));

        return synthesizedSeriesExtractor;
    }

    private BtVodSeriesProvider getSeriesProvider(MultiplexingVodContentListener listeners,
            TopicUpdater topicUpdater, BtVodBrandProvider brandProvider,
            BtVodExplicitSeriesExtractor explicitSeriesExtractor,
            BtVodSynthesizedSeriesExtractor synthesizedSeriesExtractor) {
        return new BtVodSeriesProvider(
                explicitSeriesExtractor.getExplicitSeries(),
                synthesizedSeriesExtractor.getSynthesizedSeries(),
                seriesUriExtractor,
                new HierarchyDescriptionAndImageUpdater(),
                new CertificateUpdater(),
                brandProvider,
                topicUpdater,
                listeners
        );
    }

    private BtVodItemExtractor extractItems(MultiplexingVodContentListener listeners,
            Set<String> processedRows, BtVodBrandProvider brandProvider,
            BtVodSeriesProvider seriesProvider, ProgressReporter reporter) throws IOException {
        reporter.updateItemExtractStatus(ProgressReporter.IN_PROGRESS);

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
                new DedupedDescriptionAndImageUpdater(),
                new BtVodMpxBackedEpisodeNumberExtractor(mpxClient),
                mpxClient,
                kidsPredicate
        );

        vodData.processData(itemExtractor);

        reporter.updateItemExtractStatus(String.format(
                "[DONE: %d rows successful, %d rows failed, %d items extracted]",
                itemExtractor.getResult().getProcessed(),
                itemExtractor.getResult().getFailures(),
                itemExtractor.getProcessedItems().size()
        ));

        return itemExtractor;
    }

    private void writeContent(BtVodBrandProvider brandProvider, BtVodSeriesProvider seriesProvider,
            BtVodItemExtractor itemExtractor, ProgressReporter reporter) {
        reporter.updateContentWritingStatus(ProgressReporter.IN_PROGRESS);

        writeContent(
                Iterables.concat(
                        brandProvider.getBrands(),
                        seriesProvider.getExplicitSeries(),
                        seriesProvider.getSynthesisedSeries(),
                        itemExtractor.getProcessedItems().values()
                )
        );

        reporter.updateContentWritingStatus(ProgressReporter.DONE);
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

    private void writeContentGroups(MultiplexingVodContentListener listeners,
            ProgressReporter reporter) {
        reporter.updateContentGroupWritingStatus(ProgressReporter.IN_PROGRESS);

        listeners.afterContent();

        reporter.updateContentGroupWritingStatus(ProgressReporter.DONE);
    }

    private class ProgressReporter {

        public static final String TODO = "[TODO]";
        public static final String IN_PROGRESS = "[IN PROGRESS]";
        public static final String DONE = "[DONE]";

        private String brandImagesExtractStatus = TODO;
        private String brandExtractStatus = TODO;
        private String collectionExtractStatus = TODO;
        private String explicitSeriesExtractStatus = TODO;
        private String synthesizedSeriesExtractStatus = TODO;
        private String itemExtractStatus = TODO;
        private String contentWritingStatus = TODO;
        private String contentGroupWritingStatus = TODO;

        public void updateBrandImagesExtractStatus(String brandImagesExtractStatus) {
            this.brandImagesExtractStatus = brandImagesExtractStatus;
            report();
        }

        public void updateBrandExtractStatus(String brandExtractStatus) {
            this.brandExtractStatus = brandExtractStatus;
            report();
        }

        public void updateCollectionExtractStatus(String collectionExtractStatus) {
            this.collectionExtractStatus = collectionExtractStatus;
            report();
        }

        public void updateExplicitSeriesExtractStatus(String explicitSeriesExtractStatus) {
            this.explicitSeriesExtractStatus = explicitSeriesExtractStatus;
            report();
        }

        public void updateSynthesizedSeriesExtractStatus(String synthesizedSeriesExtractStatus) {
            this.synthesizedSeriesExtractStatus = synthesizedSeriesExtractStatus;
            report();
        }

        public void updateItemExtractStatus(String itemExtractStatus) {
            this.itemExtractStatus = itemExtractStatus;
            report();
        }

        public void updateContentWritingStatus(String contentWritingStatus) {
            this.contentWritingStatus = contentWritingStatus;
            report();
        }

        public void updateContentGroupWritingStatus(String contentGroupWritingStatus) {
            this.contentGroupWritingStatus = contentGroupWritingStatus;
            report();
        }

        public void report() {
            reportStatus(
                    String.format(
                            "Brand images extract %s, "
                                    + "Brand extract %s, "
                                    + "Collection extract %s, "
                                    + "Explicit series extract %s, "
                                    + "Synthesized series extract %s, "
                                    + "Item extract %s, "
                                    + "Content writing %s, "
                                    + "Content group writing %s",
                            brandImagesExtractStatus,
                            brandExtractStatus,
                            collectionExtractStatus,
                            explicitSeriesExtractStatus,
                            synthesizedSeriesExtractStatus,
                            itemExtractStatus,
                            contentWritingStatus,
                            contentGroupWritingStatus
                    )
            );
        }
    }
}

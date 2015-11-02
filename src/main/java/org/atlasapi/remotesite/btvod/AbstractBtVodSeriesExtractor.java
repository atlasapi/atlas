package org.atlasapi.remotesite.btvod;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Map;
import java.util.Set;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Series;
import org.atlasapi.media.entity.TopicRef;
import org.atlasapi.remotesite.btvod.model.BtVodEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.metabroadcast.common.scheduling.UpdateProgress;

public abstract class AbstractBtVodSeriesExtractor implements BtVodDataProcessor<UpdateProgress> {

    private static final Logger log = LoggerFactory.getLogger(AbstractBtVodSeriesExtractor.class);

    private final BtVodBrandProvider brandProvider;
    private final Publisher publisher;
    private final Map<String, Series> processedSeries = Maps.newHashMap();
    private final BtVodContentListener listener;
    private final Set<String> processedRows;
    private final BtVodDescribedFieldsExtractor describedFieldsExtractor;
    private final ImageExtractor imageExtractor;
    private final BtVodSeriesUriExtractor seriesUriExtractor;
    private final ImmutableSet<TopicRef> topicsToPropagateToParents;

    private UpdateProgress progress = UpdateProgress.START;

    protected AbstractBtVodSeriesExtractor(
            BtVodBrandProvider brandProvider,
            Publisher publisher,
            BtVodContentListener listener,
            Set<String> processedRows,
            BtVodDescribedFieldsExtractor describedFieldsExtractor,
            BtVodSeriesUriExtractor seriesUriExtractor,
            ImageExtractor imageExtractor,
            Iterable<TopicRef> topicsToPropagateToParents
    ) {
        this.processedRows = checkNotNull(processedRows);
        this.listener = checkNotNull(listener);
        this.brandProvider = checkNotNull(brandProvider);
        this.publisher = checkNotNull(publisher);
        this.seriesUriExtractor = checkNotNull(seriesUriExtractor);
        this.imageExtractor = checkNotNull(imageExtractor);
        //TODO: Use DescribedFieldsExtractor for all described fields, not just aliases.
        //      Added as a collaborator for Alias extraction, but should be used more
        //      widely
        this.describedFieldsExtractor = checkNotNull(describedFieldsExtractor);
        this.topicsToPropagateToParents = ImmutableSet.copyOf(topicsToPropagateToParents);
    }

    @Override
    public boolean process(BtVodEntry row) {
        UpdateProgress thisProgress = UpdateProgress.FAILURE;
        try {
            if (!shouldProcess(row)) {
                thisProgress = UpdateProgress.SUCCESS;
                return true;
            }

            Series series;
            if (processedSeries.containsKey(seriesUriExtractor.seriesUriFor(row).get())) {
                series = processedSeries.get(seriesUriExtractor.seriesUriFor(row).get());
            } else {
                series = seriesFrom(row);
                brandProvider.updateDescription(row, series);
            }
            setFields(series, row);
            setAdditionalFields(series, row);
            addTopicsToParents(series, row);
            onSeriesProcessed(series, row);
            // This allows a lookup by series title. Note that the only reference from an episode to a series is the series title.
            // Consequently this map will be used to lookup SeriesRef when processing episodes
            // TODO: is there a better approach than this ^?
            processedSeries.put(seriesUriExtractor.seriesUriFor(row).get(), series);
            processedRows.add(seriesUriExtractor.seriesUriFor(row).get());
            listener.onContent(series, row);
            thisProgress = UpdateProgress.SUCCESS;
        } catch (Exception e) {
            log.error("Failed to process row: " + row.toString(), e);
        } finally {
            progress = progress.reduce(thisProgress);
        }
        return true;
    }

    private void setFields(Series series, BtVodEntry row) {
        VodEntryAndContent vodEntryAndContent = new VodEntryAndContent(row, series);
        series.addTopicRefs(describedFieldsExtractor.topicsFrom(vodEntryAndContent));
    }
    
    //TODO Factor this out rather than duplicate from BtVodItemExtractor
    private void addTopicsToParents(Series series, BtVodEntry row) {
        for (TopicRef topicRef : topicsToPropagateToParents) {
            if (series.getTopicRefs().contains(topicRef)) {
                if (series.getParent() != null) {
                    Brand brand = brandProvider.brandFor(row).get();
                    brand.addTopicRef(topicRef);
                    listener.onContent(brand, row);
                }
            }
        }
    }

    @Override
    public UpdateProgress getResult() {
        return progress;
    }

    public Optional<Series> getSeriesFor(BtVodEntry row) {
        Optional<String> seriesUri = seriesUriExtractor.seriesUriFor(row);
        if (seriesUri.isPresent()) {
            return Optional.fromNullable(processedSeries.get(seriesUri.get()));
        }
        return Optional.absent();
    }

    protected abstract boolean shouldProcess(BtVodEntry row);

    protected abstract void onSeriesProcessed(Series series, BtVodEntry row);

    protected abstract void setAdditionalFields(Series series, BtVodEntry row);

    private Series seriesFrom(BtVodEntry row) {
        Series series = new Series(seriesUriExtractor.seriesUriFor(row).get(), null, publisher);
        //TODO more fields
        series.withSeriesNumber(seriesUriExtractor.extractSeriesNumber(row).get());
        series.setParentRef(brandProvider.brandRefFor(row).orNull());
        series.setGenres(describedFieldsExtractor.btGenreStringsFrom(row));
        VodEntryAndContent vodEntryAndContent = new VodEntryAndContent(row, series);
        series.addTopicRefs(describedFieldsExtractor.topicsFrom(vodEntryAndContent));
        series.setImages(imageExtractor.imagesFor(row));

        return series;
    }



    protected BtVodSeriesUriExtractor getSeriesUriExtractor() {
        return seriesUriExtractor;
    }

    protected BtVodDescribedFieldsExtractor getDescribedFieldsExtractor() {
        return describedFieldsExtractor;
    }

    protected Set<String> getProcessedRows() {
        return ImmutableSet.copyOf(processedRows);
    }

}

package org.atlasapi.remotesite.btvod;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.scheduling.UpdateProgress;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Series;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.remotesite.ContentMerger;
import org.atlasapi.remotesite.btvod.model.BtVodEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

public abstract class AbstractBtVodSeriesWriter implements BtVodDataProcessor<UpdateProgress> {

    private static final Logger log = LoggerFactory.getLogger(AbstractBtVodSeriesWriter.class);

    private final ContentWriter writer;
    private final ContentResolver resolver;

    private final BtVodBrandWriter brandExtractor;

    private final Publisher publisher;
    private final ContentMerger contentMerger;
    private final Map<String, Series> processedSeries = Maps.newHashMap();
    private final BtVodContentListener listener;
    private final Set<String> processedRows;
    private final BtVodDescribedFieldsExtractor describedFieldsExtractor;
    private final BtVodSeriesUriExtractor seriesUriExtractor;
    private UpdateProgress progress = UpdateProgress.START;

    protected AbstractBtVodSeriesWriter(
            ContentWriter writer,
            ContentResolver resolver,
            BtVodBrandWriter brandExtractor,
            Publisher publisher,
            BtVodContentListener listener,
            Set<String> processedRows,
            BtVodDescribedFieldsExtractor describedFieldsExtractor,
            BtVodSeriesUriExtractor seriesUriExtractor
    ) {
        this.processedRows = checkNotNull(processedRows);
        this.listener = checkNotNull(listener);
        this.writer = checkNotNull(writer);
        this.resolver = checkNotNull(resolver);
        this.brandExtractor = checkNotNull(brandExtractor);
        this.publisher = checkNotNull(publisher);
        this.seriesUriExtractor = checkNotNull(seriesUriExtractor);
        //TODO: Use DescribedFieldsExtractor for all described fields, not just aliases.
        //      Added as a collaborator for Alias extraction, but should be used more
        //      widely
        this.describedFieldsExtractor = checkNotNull(describedFieldsExtractor);
        this.contentMerger = new ContentMerger(ContentMerger.MergeStrategy.REPLACE, ContentMerger.MergeStrategy.REPLACE, ContentMerger.MergeStrategy.REPLACE);
    }

    @Override
    public boolean process(BtVodEntry row) {
        UpdateProgress thisProgress = UpdateProgress.FAILURE;
        try {
            if (!shouldProcess(row) || processedRows.contains(seriesUriExtractor.seriesUriFor(row).get())) {
                thisProgress = UpdateProgress.SUCCESS;
                return true;
            }

            Series series = seriesFrom(row);
            setAdditionalFields(series, row);
            write(series);
            onSeriesProcessed(series, row);

            // This allows a lookup by series title. Note that the only reference from an episode to a series is the series title.
            // Consequently this map will be used to lookup SeriesRef when processing episodes
            // TODO: is there a better approach than this ^?
            processedSeries.put(seriesUriExtractor.seriesUriFor(row).get(), series);
            listener.onContent(series, row);
            thisProgress = UpdateProgress.SUCCESS;
        } catch (Exception e) {
            log.error("Failed to process row: " + row.toString(), e);
        } finally {
            progress = progress.reduce(thisProgress);
        }
        return true;
    }

    @Override
    public UpdateProgress getResult() {
        return progress;
    }


    public Optional<Series> getSeriesFor(BtVodEntry row) {
        Optional<String> seriesUri = seriesUriExtractor.seriesUriFor(row);
        if(seriesUri.isPresent()) {
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
        series.setParentRef(brandExtractor.getBrandRefFor(row).orNull());
        series.setAliases(describedFieldsExtractor.aliasesFrom(row));
        series.setGenres(describedFieldsExtractor.btGenreStringsFrom(row));
        VodEntryAndContent vodEntryAndContent = new VodEntryAndContent(row, series);
        series.addTopicRefs(describedFieldsExtractor.topicsFrom(vodEntryAndContent));

        return series;
    }

    private void write(Series extracted) {
        Maybe<Identified> existing = resolver
                .findByCanonicalUris(ImmutableSet.of(extracted.getCanonicalUri()))
                .getFirstValue();

        if (existing.hasValue()) {
            Container merged = contentMerger.merge((Series) existing.requireValue(),
                    extracted);
            writer.createOrUpdate(merged);
        } else {
            writer.createOrUpdate(extracted);
        }
    }

    protected BtVodSeriesUriExtractor getSeriesUriExtractor() {
        return seriesUriExtractor;
    }

}

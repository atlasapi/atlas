package org.atlasapi.remotesite.btvod;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.primitives.Ints;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.ParentRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Series;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.remotesite.ContentMerger;
import org.atlasapi.remotesite.ContentMerger.MergeStrategy;
import org.atlasapi.remotesite.btvod.model.BtVodEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.scheduling.UpdateProgress;


public class BtVodSeriesWriter implements BtVodDataProcessor<UpdateProgress>{

    private static final Logger log = LoggerFactory.getLogger(BtVodSeriesWriter.class);
    private static final Pattern SERIES_NUMBER_PATTERN = Pattern.compile("S([0-9]+)\\-E[0-9]+");


    private final ContentWriter writer;
    private final ContentResolver resolver;
    private final BtVodBrandWriter brandExtractor;
    private final Publisher publisher;
    private final ContentMerger contentMerger;
    private final Map<String, ParentRef> processedSeries = Maps.newHashMap();
    private final BtVodContentListener listener;
    private final Set<String> processedRows;
    private final BtVodDescribedFieldsExtractor describedFieldsExtractor;
    private UpdateProgress progress = UpdateProgress.START;


    public BtVodSeriesWriter(
            ContentWriter writer,
            ContentResolver resolver,
            BtVodBrandWriter brandExtractor,
            BtVodDescribedFieldsExtractor describedFieldsExtractor,
            Publisher publisher,
            BtVodContentListener listener,
            Set<String> processedRows
    ) {
        this.processedRows = checkNotNull(processedRows);
        this.listener = checkNotNull(listener);
        this.writer = checkNotNull(writer);
        this.resolver = checkNotNull(resolver);
        this.brandExtractor = checkNotNull(brandExtractor);
        this.publisher = checkNotNull(publisher);
        this.describedFieldsExtractor = checkNotNull(describedFieldsExtractor);
        this.contentMerger = new ContentMerger(MergeStrategy.REPLACE, MergeStrategy.KEEP, MergeStrategy.REPLACE);
    }
    
    @Override
    public boolean process(BtVodEntry row) {
        UpdateProgress thisProgress = UpdateProgress.FAILURE;
        try {
            if (!isPartOfSeries(row)
                    || processedRows.contains(uriFor(row))) {
                thisProgress = UpdateProgress.SUCCESS;
                return true;
            }

            Series series = seriesFrom(row);
            write(series);

            // This allows a lookup by series title. Note that the only reference from an episode to a series is the series title.
            // Consequently this map will be used to lookup SeriesRef when processing episodes
            // TODO: is there a better approach than this ^?
            processedSeries.put(uriFor(row), ParentRef.parentRefFrom(series));
            listener.onContent(series, row);
            thisProgress = UpdateProgress.SUCCESS;
        } catch (Exception e) {
            log.error("Failed to process row: " + row.toString(), e);
        } finally {
            progress = progress.reduce(thisProgress);
        }
        return true;
    }

    private boolean isPartOfSeries(BtVodEntry row) {
        //TODO implement
        return extractSeriesNumber(row.getTitle()) == null;
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

    private Series seriesFrom(BtVodEntry row) {
        Series series = new Series(uriFor(row), null, publisher);
        //TODO more fields
        series.withSeriesNumber(extractSeriesNumber(row.getTitle()));
        describedFieldsExtractor.setDescribedFieldsFrom(row, series);
        series.setParentRef(brandExtractor.getBrandRefFor(row).orNull());
        return series;
    }

    public Integer extractSeriesNumber(String title) {
        if (title == null) {
            return null;
        }

        Matcher matcher = SERIES_NUMBER_PATTERN.matcher(title);

        if (matcher.find()) {
            return Ints.tryParse(matcher.group(1));
        }

        return null;
    }

    public String uriFor(BtVodEntry row) {
        Integer seriesNumber = extractSeriesNumber(row.getTitle());
        return brandExtractor.uriFor(row) +  "/series/" + seriesNumber;
    }
    
    @Override
    public UpdateProgress getResult() {
        return progress;
    }

    public Optional<ParentRef> getSeriesRefFor(BtVodEntry row) {
        return Optional.fromNullable(processedSeries.get(uriFor(row)));
    }

}

package org.atlasapi.remotesite.btvod;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.ParentRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Series;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.remotesite.ContentMerger;
import org.atlasapi.remotesite.ContentMerger.MergeStrategy;
import org.atlasapi.remotesite.btvod.BtVodData.BtVodDataRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.scheduling.UpdateProgress;


public class BtVodSeriesWriter implements BtVodDataProcessor<UpdateProgress>{

    private static final Logger log = LoggerFactory.getLogger(BtVodSeriesWriter.class);

    private final ContentWriter writer;
    private final ContentResolver resolver;
    private final BtVodBrandWriter brandExtractor;
    private final Publisher publisher;
    private final String uriPrefix;
    private final ContentMerger contentMerger;
    private final Map<String, ParentRef> processedSeries = Maps.newHashMap();
    private final BtVodContentListener listener;
    private final Set<String> processedRows;
    private final BtVodDescribedFieldsExtractor describedFieldsExtractor;
    private UpdateProgress progress = UpdateProgress.START;


    public BtVodSeriesWriter(ContentWriter writer, ContentResolver resolver,
            BtVodBrandWriter brandExtractor, 
            BtVodDescribedFieldsExtractor describedFieldsExtractor, Publisher publisher, 
            String uriPrefix, BtVodContentListener listener, 
            Set<String> processedRows) {
        this.processedRows = checkNotNull(processedRows);
        this.listener = checkNotNull(listener);
        this.writer = checkNotNull(writer);
        this.resolver = checkNotNull(resolver);
        this.brandExtractor = checkNotNull(brandExtractor);
        this.publisher = checkNotNull(publisher);
        this.uriPrefix = checkNotNull(uriPrefix);
        this.describedFieldsExtractor = checkNotNull(describedFieldsExtractor);
        this.contentMerger = new ContentMerger(MergeStrategy.REPLACE, MergeStrategy.KEEP, MergeStrategy.REPLACE);
    }
    
    @Override
    public boolean process(BtVodDataRow row) {
        UpdateProgress thisProgress = UpdateProgress.FAILURE;
        try {
            if (!"Y".equals(row.getColumnValue(BtVodFileColumn.IS_SERIES))
                    || processedRows.contains(row.getColumnValue(BtVodFileColumn.PRODUCT_ID))) {
                thisProgress = UpdateProgress.SUCCESS;
                return true;
            }
            
            Series series = seriesFrom(row);
            write(series);

            // This allows a lookup by series title. Note that the only reference from an episode to a series is the series title.
            // Consequently this map will be used to lookup SeriesRef when processing episodes
            // TODO: is there a better approach than this ^?
            processedSeries.put(series.getTitle(), ParentRef.parentRefFrom(series));
            listener.onContent(series, row);
            thisProgress = UpdateProgress.SUCCESS;
        } catch (Exception e) {
            log.error("Failed to process row: " + row.toString(), e);
        } finally {
            progress = progress.reduce(thisProgress);
        }
        return true;
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

    private Series seriesFrom(BtVodDataRow row) {
        Series series = new Series(uriFor(row), null, publisher);
        series.setTitle(row.getColumnValue(BtVodFileColumn.SERIES_TITLE));
        //TODO more fields
        Integer seriesNumber = 
                Integer.parseInt(row.getColumnValue(BtVodFileColumn.SERIES_NUMBER));
        series.withSeriesNumber(seriesNumber);
        series.setTitle(row.getColumnValue(BtVodFileColumn.PRODUCT_TITLE));
        describedFieldsExtractor.setDescribedFieldsFrom(row, series);
        series.setParentRef(brandExtractor.getBrandRefFor(row).orNull());
        return series;
    }
    
    private String uriFor(BtVodDataRow row) {
        String seriesId = row.getColumnValue(BtVodFileColumn.PRODUCT_ID);
        return uriPrefix + "series/" + seriesId;
    }
    
    @Override
    public UpdateProgress getResult() {
        return progress;
    }

    public Optional<ParentRef> getSeriesRefFor(String seriesTitle) {
        return Optional.fromNullable(processedSeries.get(seriesTitle));
    }

}

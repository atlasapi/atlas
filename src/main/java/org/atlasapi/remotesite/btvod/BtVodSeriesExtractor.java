package org.atlasapi.remotesite.btvod;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Map;

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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.scheduling.UpdateProgress;


public class BtVodSeriesExtractor implements BtVodDataProcessor<UpdateProgress>{

    private static final Logger log = LoggerFactory.getLogger(BtVodSeriesExtractor.class);
    
    private final ContentWriter writer;
    private final ContentResolver resolver;
    private final BtVodBrandExtractor brandExtractor;
    private final Publisher publisher;
    private final String uriPrefix;
    private final ContentMerger contentMerger;
    private final Map<String, ParentRef> processedSeries = Maps.newHashMap();
    private final BtVodContentListener listener;
    private UpdateProgress progress = UpdateProgress.START;

    public BtVodSeriesExtractor(ContentWriter writer, ContentResolver resolver,
            BtVodBrandExtractor brandExtractor, Publisher publisher, 
            String uriPrefix, BtVodContentListener listener) {
        this.listener = checkNotNull(listener);
        this.writer = checkNotNull(writer);
        this.resolver = checkNotNull(resolver);
        this.brandExtractor = checkNotNull(brandExtractor);
        this.publisher = checkNotNull(publisher);
        this.uriPrefix = checkNotNull(uriPrefix);
        this.contentMerger = new ContentMerger(MergeStrategy.REPLACE, MergeStrategy.KEEP, MergeStrategy.REPLACE);
    }
    
    @Override
    public boolean process(BtVodDataRow row) {
        UpdateProgress thisProgress = UpdateProgress.FAILURE;
        try {
            if (!"Y".equals(row.getColumnValue(BtVodFileColumn.IS_SERIES))) {
                thisProgress = UpdateProgress.SUCCESS;
                return true;
            }
            
            Series series = seriesFrom(row);
            write(series);
            
            String brandId = row.getColumnValue(BtVodFileColumn.BRANDIA_ID);
            processedSeries.put(
                    seriesKeyFor(brandId, series.getSeriesNumber()), 
                    ParentRef.parentRefFrom(series));
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
        
        series.setParentRef(brandExtractor.getBrandRefFor(row.getColumnValue(BtVodFileColumn.BRANDIA_ID)));
        return series;
    }
    
    private String uriFor(BtVodDataRow row) {
        String brandId = row.getColumnValue(BtVodFileColumn.BRANDIA_ID);
        return uriPrefix + "series/" + brandId;
    }
    
    @Override
    public UpdateProgress getResult() {
        return progress;
    }

    public ParentRef getSeriesRefFor(String brandId, int seriesNumber) {
        return processedSeries.get(seriesKeyFor(brandId, seriesNumber));
    }
    
    private String seriesKeyFor(String brandId, int seriesNumber) {
        return brandId + "-" + seriesNumber;
    }
}

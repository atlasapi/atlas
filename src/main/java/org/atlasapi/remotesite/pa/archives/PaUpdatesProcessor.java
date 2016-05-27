package org.atlasapi.remotesite.pa.archives;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Series;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.remotesite.pa.listings.bindings.ProgData;

import com.metabroadcast.common.time.Timestamp;

import com.google.common.base.Optional;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class PaUpdatesProcessor {

    private static final Logger log = LoggerFactory.getLogger(PaUpdatesProcessor.class);
    private final PaProgDataUpdatesProcessor processor;
    private final ContentWriter contentWriter;

    private PaUpdatesProcessor(PaProgDataUpdatesProcessor processor, ContentWriter contentWriter) {
        this.processor = checkNotNull(processor);
        this.contentWriter = checkNotNull(contentWriter);
    }

    public static PaUpdatesProcessor create(
            PaProgDataUpdatesProcessor processor,
            ContentWriter contentWriter
    ) {
        return new PaUpdatesProcessor(processor, contentWriter);
    }

    public void process(ProgData progData, DateTimeZone zone, Timestamp timestamp) {
        try {
            Optional<ContentHierarchyWithoutBroadcast> hierarchy = processor.process(
                    progData, zone, timestamp
            );
            if (hierarchy.isPresent()) {
                writeHierarchy(hierarchy.get());
            }

        } catch (Exception e) {
            log.error(String.format("Error processing prog id %s", progData.getProgId()));
        }
    }

    private void writeHierarchy(ContentHierarchyWithoutBroadcast hierarchy) {
        Optional<Brand> brandOptional = hierarchy.getBrand();
        Optional<Series> seriesOptional = hierarchy.getSeries();
        Item item = hierarchy.getItem();

        setSeriesParent(brandOptional, seriesOptional);
        setItemContainer(item, brandOptional, seriesOptional);
        setItemSeries(item, brandOptional, seriesOptional);

        if (brandOptional.isPresent()) {
            contentWriter.createOrUpdate(brandOptional.get());
        }
        if (seriesOptional.isPresent()) {
            contentWriter.createOrUpdate(seriesOptional.get());
        }
        contentWriter.createOrUpdate(item);

        Optional<Brand> brandSummary = hierarchy.getBrandSummary();
        Optional<Series> seriesSummary = hierarchy.getSeriesSummary();

        if (brandSummary.isPresent()) {
            contentWriter.createOrUpdate(brandSummary.get());
        }

        if (seriesSummary.isPresent()) {
            contentWriter.createOrUpdate(seriesSummary.get());
        }
    }

    private void setSeriesParent(Optional<Brand> brandOptional, Optional<Series> seriesOptional) {
        if (brandOptional.isPresent() && seriesOptional.isPresent()) {
            seriesOptional.get().setParent(brandOptional.get());
        }
    }

    private void setItemContainer(Item item, Optional<Brand> brandOptional,
            Optional<Series> seriesOptional) {
        if (brandOptional.isPresent()) {
            item.setContainer(brandOptional.get());
        } else if (seriesOptional.isPresent()) {
            item.setContainer(seriesOptional.get());
        }
    }

    private void setItemSeries(Item item, Optional<Brand> brandOptional,
            Optional<Series> seriesOptional) {
        if (brandOptional.isPresent() && seriesOptional.isPresent() && item instanceof Episode) {
            ((Episode) item).setSeries(seriesOptional.get());
        }
    }
}

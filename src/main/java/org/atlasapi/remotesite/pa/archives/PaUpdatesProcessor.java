package org.atlasapi.remotesite.pa.archives;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Series;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.remotesite.pa.ContentHierarchyAndSummaries;
import org.atlasapi.remotesite.pa.listings.bindings.ProgData;

import com.metabroadcast.common.time.Timestamp;

import com.google.common.base.Optional;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PaUpdatesProcessor {

    private static final Logger log = LoggerFactory.getLogger(PaUpdatesProcessor.class);
    private final PaProgDataUpdatesProcessor processor;
    private final ContentWriter contentWriter;

    public PaUpdatesProcessor(PaProgDataUpdatesProcessor processor, ContentWriter contentWriter) {
        this.processor = processor;
        this.contentWriter = contentWriter;
    }

    public void process(ProgData progData, DateTimeZone zone, Timestamp timestamp) {
        try {
            ContentHierarchyWithoutBroadcast hierarchy = processor.process(progData, zone, timestamp);
            Optional<Brand> brandOptional = hierarchy.getBrand();
            Optional<Series> seriesOptional = hierarchy.getSeries();
            if (brandOptional.isPresent()) {
                contentWriter.createOrUpdate(brandOptional.get());
            }

            if (seriesOptional.isPresent()) {
                contentWriter.createOrUpdate(seriesOptional.get());
            }

            contentWriter.createOrUpdate(hierarchy.getItem());
        } catch (Exception e) {
            log.error(String.format("Error processing prog id %s", progData.getProgId()));
        }

    }

}

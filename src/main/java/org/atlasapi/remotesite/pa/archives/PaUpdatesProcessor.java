package org.atlasapi.remotesite.pa.archives;

import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.remotesite.pa.ContentHierarchyAndSummaries;
import org.atlasapi.remotesite.pa.PaProgDataProcessor;
import org.atlasapi.remotesite.pa.PaProgrammeProcessor;
import org.atlasapi.remotesite.pa.listings.bindings.ProgData;

import com.metabroadcast.common.time.Timestamp;

import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PaUpdatesProcessor {

    private static final Logger log = LoggerFactory.getLogger(PaUpdatesProcessor.class);
    private final PaProgDataProcessor processor;
    private final ContentWriter contentWriter;

    public PaUpdatesProcessor(PaProgDataProcessor processor, ContentWriter contentWriter) {
        this.processor = processor;
        this.contentWriter = contentWriter;
    }

    public void process(ProgData progData, DateTimeZone zone, Timestamp timestamp) {
        ContentHierarchyAndSummaries hierarchy = processor.process(progData, null, zone, timestamp);
    }

}

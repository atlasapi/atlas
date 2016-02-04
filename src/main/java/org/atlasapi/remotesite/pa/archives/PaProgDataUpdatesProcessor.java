package org.atlasapi.remotesite.pa.archives;

import org.atlasapi.remotesite.pa.ContentHierarchyAndSummaries;
import org.atlasapi.remotesite.pa.listings.bindings.ProgData;

import com.metabroadcast.common.time.Timestamp;

import org.joda.time.DateTimeZone;

public interface PaProgDataUpdatesProcessor {

     ContentHierarchyAndSummaries process(ProgData progData, DateTimeZone zone, Timestamp updatedAt);

}

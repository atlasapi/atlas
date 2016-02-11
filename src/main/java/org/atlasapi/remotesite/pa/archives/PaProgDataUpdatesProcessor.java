package org.atlasapi.remotesite.pa.archives;

import org.atlasapi.remotesite.pa.ContentHierarchyAndSummaries;
import org.atlasapi.remotesite.pa.listings.bindings.ProgData;

import com.metabroadcast.common.time.Timestamp;

import com.google.common.base.Optional;
import org.joda.time.DateTimeZone;

public interface PaProgDataUpdatesProcessor {

     Optional<ContentHierarchyWithoutBroadcast> process(ProgData progData, DateTimeZone zone, Timestamp updatedAt);

}

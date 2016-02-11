package org.atlasapi.remotesite.pa;

import org.atlasapi.media.channel.Channel;
import org.atlasapi.remotesite.pa.listings.bindings.ProgData;

import com.google.common.base.Optional;
import org.joda.time.DateTimeZone;

import com.metabroadcast.common.time.Timestamp;

public interface PaProgDataProcessor {

    public Optional<ContentHierarchyAndSummaries> process(ProgData progData, Channel channel, DateTimeZone zone, Timestamp updatedAt);

}
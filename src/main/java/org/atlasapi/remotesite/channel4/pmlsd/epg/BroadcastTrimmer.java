package org.atlasapi.remotesite.channel4.pmlsd.epg;

import java.util.Map;

import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.entity.Publisher;
import org.joda.time.Interval;

public interface BroadcastTrimmer {

    void trimBroadcasts(Interval scheduleInterval, Channel channel, Map<String, String> acceptableIds);
    void trimBroadcasts(Publisher publisher, Interval scheduleInterval, Channel channel, Map<String, String> acceptableIds);
    
}
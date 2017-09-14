package org.atlasapi.remotesite.youview;

import nu.xom.Element;

import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.ScheduleEntry.ItemRefAndBroadcast;


public interface YouViewElementProcessor {
    
    ItemRefAndBroadcast process(Channel channel, Publisher targetPublisher, Element element);
}

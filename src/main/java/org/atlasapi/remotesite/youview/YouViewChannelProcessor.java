package org.atlasapi.remotesite.youview;

import com.metabroadcast.common.scheduling.UpdateProgress;
import nu.xom.Element;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.entity.Publisher;
import org.joda.time.Interval;

import java.util.List;

public interface YouViewChannelProcessor {
    UpdateProgress process(Channel channel, Publisher targetPublisher, List<Element> elements);
}

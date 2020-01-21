package org.atlasapi.remotesite.youview;

import com.metabroadcast.common.scheduling.UpdateProgress;
import nu.xom.Element;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.entity.Publisher;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface YouViewChannelProcessor {
    Map.Entry<UpdateProgress, Set<String>> process(Channel channel, Publisher targetPublisher, List<Element> elements);
}

package org.atlasapi.remotesite.youview;

import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import org.atlasapi.media.channel.Channel;

import java.util.Collection;
import java.util.Map;

public class DummyYouViewChannelResolver implements YouViewChannelResolver {

    private final Multimap<ServiceId, Channel> channelMap;
    private final Map<Integer, ServiceId> serviceIdMap;

    public DummyYouViewChannelResolver(Multimap<ServiceId, Channel> channelMap) {
        this.channelMap = ImmutableMultimap.copyOf(channelMap);
        this.serviceIdMap = Maps.uniqueIndex(channelMap.keySet(), ServiceId::getId);
    }
    
    @Override
    public Collection<String> getChannelUris(int channelId) {
        return Collections2.transform(
                channelMap.get(serviceIdMap.get(channelId)),
                Channel::getUri
        );
    }

    @Override
    public Collection<Channel> getChannels(int channelId) {
        return channelMap.get(serviceIdMap.get(channelId));
    }

    @Override
    public Collection<Channel> getAllChannels() {
        return channelMap.values();
    }

    @Override
    public Multimap<ServiceId, Channel> getAllServiceIdsToChannels() {
        return channelMap;
    }

    @Override public Collection<ServiceId> getServiceIds(Channel channel) {
        return channelMap.keySet();
    }

}

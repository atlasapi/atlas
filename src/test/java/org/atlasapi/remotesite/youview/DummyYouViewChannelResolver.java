package org.atlasapi.remotesite.youview;

import java.util.Collection;

import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import org.atlasapi.media.channel.Channel;

public class DummyYouViewChannelResolver implements YouViewChannelResolver {

    private final Multimap<Integer, Channel> channelMap;
    
    public DummyYouViewChannelResolver(Multimap<Integer, Channel> channelMap) {
        this.channelMap = ImmutableMultimap.copyOf(channelMap);
    }
    
    @Override
    public Collection<String> getChannelUris(int channelId) {
        return Collections2.transform(
                channelMap.get(channelId),
                Channel::getUri
        );
    }

    @Override
    public Collection<Channel> getChannels(int channelId) {
        return channelMap.get(channelId);
    }

    @Override
    public Collection<Channel> getAllChannels() {
        return channelMap.values();
    }

    @Override
    public Multimap<Integer, Channel> getAllServiceIdsToChannels() {
        return channelMap;
    }

    @Override public Collection<String> getServiceAliases(Channel channel) {
        throw new UnsupportedOperationException();
    }

    @Override public Collection<Integer> getServiceIds(Channel channel) {
        throw new UnsupportedOperationException();
    }

}

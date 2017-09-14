package org.atlasapi.remotesite.youview;

import java.util.Collection;
import java.util.Optional;

import com.google.common.collect.Multimap;
import org.atlasapi.media.channel.Channel;

public interface YouViewChannelResolver {

    @Deprecated
    Collection<String> getChannelUris(int channelId);

    @Deprecated
    Collection<Channel> getChannels(int channelId);
    
    Collection<Channel> getAllChannels();

    Multimap<Integer, Channel> getAllServiceIdsToChannels();

    Collection<String> getServiceAliases(Channel channel);

    Collection<Integer> getServiceIds(Channel channel);

}

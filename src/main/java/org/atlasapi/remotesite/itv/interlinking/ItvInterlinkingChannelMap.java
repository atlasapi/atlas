package org.atlasapi.remotesite.itv.interlinking;

import java.util.Map;

import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelResolver;

import com.google.common.collect.Maps;

public class ItvInterlinkingChannelMap {
    private Map<String, Channel> idToChannel = Maps.newHashMap();

    public ItvInterlinkingChannelMap(ChannelResolver channelResolver) {
    	
        idToChannel.put("ITV1", channelResolver.fromUri("http://www.itv.com/channels/itv1/london").requireValue());
        idToChannel.put("ITV2", channelResolver.fromUri("http://www.itv.com/channels/itv2").requireValue());
        idToChannel.put("ITV3", channelResolver.fromUri("http://www.itv.com/channels/itv3").requireValue());
        idToChannel.put("ITV4", channelResolver.fromUri("http://www.itv.com/channels/itv4").requireValue());
        idToChannel.put("CITV", channelResolver.fromUri("http://www.itv.com/channels/citv").requireValue());
        idToChannel.put("ITVBE", channelResolver.fromUri("http://ref.atlasapi.org/channels/pressassociation.com/1900").requireValue());
    }
    
    public Channel get(String key) {
        return idToChannel.get(key);
    }
}

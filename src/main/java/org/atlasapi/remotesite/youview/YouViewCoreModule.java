package org.atlasapi.remotesite.youview;

import java.util.Set;

import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.remotesite.pa.channels.PaChannelsIngester;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class YouViewCoreModule {

    protected static final String SCOTLAND_SERVICE_ALIAS_PREFIX = "http://scotlandradio.youview.com/service/";
    private static final Set<String> ALIAS_PREFIXES = Sets.union(
            ImmutableSet.of(SCOTLAND_SERVICE_ALIAS_PREFIX), ImmutableSet.copyOf(PaChannelsIngester.YOUVIEW_SERVICE_ID_ALIAS_PREFIXES));
    
    private @Autowired ChannelResolver channelResolver;
    
    @Bean
    public YouViewChannelResolver youviewChannelResolver() {
        return DefaultYouViewChannelResolver.create(channelResolver, ALIAS_PREFIXES);
    }
}

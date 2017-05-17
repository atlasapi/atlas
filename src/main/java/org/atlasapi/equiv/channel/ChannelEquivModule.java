package org.atlasapi.equiv.channel;

import org.atlasapi.equiv.channel.matchers.BtChannelMatcher;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.channel.ChannelWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@SuppressWarnings("PublicConstructor")
@Configuration
public class ChannelEquivModule {

    @Value("${channel.equiv.enabled}") private boolean channelEquivEnabled;

    @Autowired private ChannelResolver channelResolver;
    @Autowired private ChannelWriter channelWriter;
}

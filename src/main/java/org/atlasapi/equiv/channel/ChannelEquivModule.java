package org.atlasapi.equiv.channel;

import com.google.common.collect.ImmutableMap;
import org.atlasapi.equiv.channel.matchers.BtChannelMatcher;
import org.atlasapi.equiv.channel.matchers.ChannelMatcher;
import org.atlasapi.equiv.channel.updaters.MultipleSourceChannelEquivalenceUpdater;
import org.atlasapi.equiv.channel.updaters.SourceSpecificChannelEquivalenceUpdater;
import org.atlasapi.equiv.update.EquivalenceUpdater;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.channel.ChannelWriter;
import org.atlasapi.media.entity.Publisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

@SuppressWarnings("PublicConstructor")
@Configuration
public class ChannelEquivModule {

    private final ChannelMatcher btChannelMatcher = BtChannelMatcher.create();

    @Autowired private Map<Publisher, ChannelMatcher> channelMatchers;
    @Autowired private ChannelWriter channelWriter;
    @Autowired private ChannelResolver channelResolver;

    @Bean
    public EquivalenceUpdater<Channel> channelUpdater() {
        MultipleSourceChannelEquivalenceUpdater updaters = MultipleSourceChannelEquivalenceUpdater.create();

        register(Publisher.BT_TV_CHANNELS, updaters);
        register(Publisher.BT_TV_CHANNELS_TEST1, updaters);
        register(Publisher.BT_TV_CHANNELS_TEST2, updaters);
        register(Publisher.BT_TV_CHANNELS_REFERENCE, updaters);

        return updaters;
    }

    @Bean
    public Map<Publisher, ChannelMatcher> channelMatchers() {
        return ImmutableMap.<Publisher, ChannelMatcher>builder()
                .put(Publisher.BT_TV_CHANNELS, btChannelMatcher)
                .put(Publisher.BT_TV_CHANNELS_TEST1, btChannelMatcher)
                .put(Publisher.BT_TV_CHANNELS_TEST2, btChannelMatcher)
                .put(Publisher.BT_TV_CHANNELS_REFERENCE, btChannelMatcher)
                .build();
    }

    private void register(Publisher publisher, MultipleSourceChannelEquivalenceUpdater updater) {
        updater.register(publisher, createUpdaterFor(publisher));
    }

    private EquivalenceUpdater<Channel> createUpdaterFor(Publisher publisher) {
        return SourceSpecificChannelEquivalenceUpdater.builder()
                .forPublisher(publisher)
                .withChannelMatcher(channelMatchers.get(publisher))
                .withChannelResolver(channelResolver)
                .withChannelWriter(channelWriter)
                .build();
    }
}

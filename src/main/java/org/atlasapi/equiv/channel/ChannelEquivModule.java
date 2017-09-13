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

import javax.annotation.PostConstruct;
import java.util.Arrays;
import java.util.Map;

@SuppressWarnings("PublicConstructor")
@Configuration
public class ChannelEquivModule {

    private final Map<Publisher, ChannelMatcher> channelMatchers = ImmutableMap.of(
            Publisher.BT_TV_CHANNELS, BtChannelMatcher.create(Publisher.BT_TV_CHANNELS),
            Publisher.BT_TV_CHANNELS_TEST1, BtChannelMatcher.create(Publisher.BT_TV_CHANNELS_TEST1),
            Publisher.BT_TV_CHANNELS_TEST2, BtChannelMatcher.create(Publisher.BT_TV_CHANNELS_TEST2),
            Publisher.BT_TV_CHANNELS_REFERENCE, BtChannelMatcher.create(Publisher.BT_TV_CHANNELS_REFERENCE)
    );

    @Autowired private ChannelWriter channelWriter;
    @Autowired private ChannelResolver channelResolver;

    @Bean
    public EquivalenceUpdater<Channel> channelUpdater() {
        MultipleSourceChannelEquivalenceUpdater updaters = MultipleSourceChannelEquivalenceUpdater.create();

        registerPublisherUpdaters(
                updaters,
                Publisher.BT_TV_CHANNELS,
                Publisher.BT_TV_CHANNELS_TEST1,
                Publisher.BT_TV_CHANNELS_TEST2,
                Publisher.BT_TV_CHANNELS_REFERENCE
        );

        return updaters;
    }

    private void registerPublisherUpdaters(
            MultipleSourceChannelEquivalenceUpdater updaters,
            Publisher... publishers
    ) {
        Arrays.stream(publishers).forEach(
                publisher -> updaters.register(publisher, createUpdaterFor(publisher))
        );
    }

    private EquivalenceUpdater<Channel> createUpdaterFor(Publisher publisher) {
        return SourceSpecificChannelEquivalenceUpdater.builder()
                .forPublisher(publisher)
                .withChannelMatcher(channelMatchers.get(publisher))
                .withChannelResolver(channelResolver)
                .withChannelWriter(channelWriter)
                .withMetadata(
                        ChannelEquivalenceUpdaterMetadata.create(channelMatchers.get(publisher), publisher)
                )
                .build();
    }
}

package org.atlasapi.equiv.channel.updaters;

import com.google.api.client.repackaged.com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.stream.MoreCollectors;
import org.atlasapi.equiv.channel.matchers.BtChannelMatcher;
import org.atlasapi.equiv.channel.matchers.ChannelMatcher;
import org.atlasapi.equiv.update.EquivalenceUpdater;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelQuery;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.channel.ChannelWriter;
import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Publisher;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SourceSpecificChannelEquivalenceUpdaterTest {

    @Mock private ChannelWriter channelWriter = mock(ChannelWriter.class);
    @Mock private ChannelResolver channelResolver = mock(ChannelResolver.class);

    private ChannelMatcher btChannelMatcher = BtChannelMatcher.create();
    private Iterable<Channel> allChannels = generateAllChannels();

    private EquivalenceUpdater<Channel> btChannelsUpdater = getUpdaterFor(
            Publisher.BT_TV_CHANNELS,
            btChannelMatcher
    );

    @Before
    public void setUp() {
        // defaults
        when(channelResolver.allChannels(any(ChannelQuery.class))).thenReturn(
                StreamSupport.stream(allChannels.spliterator(), false)
                        .filter(channel -> channel.getSource().equals(Publisher.METABROADCAST))
                        .collect(MoreCollectors.toImmutableList())
        );
        when(channelWriter.createOrUpdate(any())).thenReturn(null);
    }

    @Test (expected = IllegalArgumentException.class)
    public void throwsExceptionWhenWrongSourcedChannel() throws Exception {
        btChannelsUpdater.updateEquivalences(getChannel(Publisher.BT_TV_CHANNELS_TEST1));
    }

    @Test
    public void equivalenceIsUpdatedOnChannelWhenMatching() {

        ArgumentCaptor<Channel> writtenChannelCaptor = ArgumentCaptor.forClass(Channel.class);

        Channel subjectChannel = getChannel(10L, Publisher.BT_TV_CHANNELS, "100");

        assertTrue(btChannelsUpdater.updateEquivalences(subjectChannel));

        verify(channelResolver).allChannels(any(ChannelQuery.class));
        verify(channelWriter).createOrUpdate(writtenChannelCaptor.capture());


        Channel writtenChannel = writtenChannelCaptor.getValue();
        assertThat(writtenChannel.getId(), is(30L));
        assertTrue(writtenChannel.getSameAs().contains(subjectChannel.toChannelRef()));

    }

    private Channel getChannel(Publisher publisher) {
        return getChannel(1234L, publisher, null);
    }

    private Channel getChannel(long id, Publisher publisher, String aliasValue) {

        Set<Alias> aliases = Strings.isNullOrEmpty(aliasValue)
                ? ImmutableSet.of(new Alias("default", Long.toString(id)))
                : ImmutableSet.of(new Alias("pa:channel:id", aliasValue));

        Channel channel = getChannel(publisher, aliases);
        channel.setId(id);

        return channel;
    }

    private Channel getChannel(Publisher publisher, Set<Alias> aliases) {

        return Channel.builder()
                .withUri("uri:" + publisher.key() + aliases.iterator().next().getValue())
                .withSource(publisher)
                .withAliases(aliases)
                .build();
    }

    private EquivalenceUpdater<Channel> getUpdaterFor(
            Publisher publisher,
            ChannelMatcher channelMatcher
    ) {
        return SourceSpecificChannelEquivalenceUpdater.builder()
                .forPublisher(publisher)
                .withChannelWriter(channelWriter)
                .withChannelResolver(channelResolver)
                .withChannelMatcher(channelMatcher)
                .build();
    }

    private Iterable<Channel> generateAllChannels() {
        return ImmutableList.of(
                getChannel(10L, Publisher.BT_TV_CHANNELS, "100"),
                getChannel(11L, Publisher.BT_TV_CHANNELS, "120"),
                getChannel(20L, Publisher.BT_TV_CHANNELS_TEST1, "100"),
                getChannel(21L, Publisher.BT_TV_CHANNELS_TEST1,"130"),
                getChannel(30L, Publisher.METABROADCAST, "100"),
                getChannel(31L, Publisher.METABROADCAST, "120"),
                getChannel(32L, Publisher.METABROADCAST, "130"),
                getChannel(33L, Publisher.METABROADCAST, "140")
        );
    }

}

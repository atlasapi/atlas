package org.atlasapi.equiv.channel.updaters;

import com.google.common.collect.ImmutableList;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.stream.MoreCollectors;
import org.atlasapi.equiv.channel.ChannelEquivalenceUpdaterMetadata;
import org.atlasapi.equiv.channel.matchers.BtChannelMatcher;
import org.atlasapi.equiv.channel.matchers.ChannelMatcher;
import org.atlasapi.equiv.update.EquivalenceUpdater;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelQuery;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.channel.ChannelWriter;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.reporting.telescope.OwlTelescopeReporter;

import com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;

import java.math.BigInteger;
import java.util.stream.StreamSupport;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SourceSpecificChannelEquivalenceUpdaterTest {

    @Mock private ChannelWriter channelWriter = mock(ChannelWriter.class);
    @Mock private ChannelResolver channelResolver = mock(ChannelResolver.class);
    @Mock private ChannelEquivalenceUpdaterMetadata metadata = mock(ChannelEquivalenceUpdaterMetadata.class);
    @Mock private OwlTelescopeReporter telescope = mock(OwlTelescopeReporter.class);

    private ChannelMatcher btChannelMatcher = BtChannelMatcher.create(
            Publisher.BT_TV_CHANNELS,
            ImmutableSet.of(
                    Publisher.METABROADCAST,
                    Publisher.YOUVIEW_JSON
            ));
    private SubstitutionTableNumberCodec codec = SubstitutionTableNumberCodec.lowerCaseOnly();
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
        btChannelsUpdater.updateEquivalences(getChannel(Publisher.BT_TV_CHANNELS_TEST1), telescope);
    }

    @Test
    public void equivalenceIsUpdatedOnChannelWhenMatching() {

        ArgumentCaptor<Channel> writtenChannelCaptor = ArgumentCaptor.forClass(Channel.class);

        Channel subjectChannel = getChannel(10L, Publisher.BT_TV_CHANNELS, 30L);
        Channel candidateChannel = getChannel(30L, Publisher.METABROADCAST, 30L);

        assertTrue(btChannelsUpdater.updateEquivalences(subjectChannel, telescope));

        verify(channelResolver).allChannels(any(ChannelQuery.class));
        verify(channelWriter, times(2)).createOrUpdate(writtenChannelCaptor.capture());


        Channel writtenChannel = writtenChannelCaptor.getValue();
        assertThat(writtenChannel.getId(), is(10L));
        assertTrue(writtenChannel.getSameAs().contains(candidateChannel.toChannelRef()));

    }

    private Channel getChannel(Publisher publisher) {
        return getChannel(1234L, publisher, 1234L);
    }

    private Channel getChannel(long channelId, Publisher publisher, long baseChannelId) {

        Channel channel = Channel.builder()
                .withUri(String.format("http://%s/%s", publisher.key(), codec.encode(BigInteger.valueOf(baseChannelId))))
                .withSource(publisher)
                .build();

        channel.setId(channelId);

        return channel;
    }

    private EquivalenceUpdater<Channel> getUpdaterFor(
            Publisher publisher,
            ChannelMatcher channelMatcher
    ) {
        return SourceSpecificChannelEquivalenceUpdater.builder()
                .forPublisher(publisher)
                .withChannelWriter(channelWriter)
                .withCandidateSources(ImmutableSet.of(Publisher.METABROADCAST))
                .withChannelResolver(channelResolver)
                .withChannelMatcher(channelMatcher)
                .withMetadata(metadata)
                .build();
    }

    private Iterable<Channel> generateAllChannels() {
        return ImmutableList.of(
                getChannel(10L, Publisher.BT_TV_CHANNELS, 30L),
                getChannel(11L, Publisher.BT_TV_CHANNELS, 31L),
                getChannel(20L, Publisher.BT_TV_CHANNELS_TEST1, 30L),
                getChannel(21L, Publisher.BT_TV_CHANNELS_TEST1, 32L),
                getChannel(30L, Publisher.METABROADCAST, 30L),
                getChannel(31L, Publisher.METABROADCAST, 31L),
                getChannel(32L, Publisher.METABROADCAST, 32L),
                getChannel(33L, Publisher.METABROADCAST, 33L)
        );
    }

}

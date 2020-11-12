package org.atlasapi.equiv.channel.matchers;

import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.entity.Publisher;

import com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.Test;

import static java.lang.String.format;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BtChannelMatcherTest {

    private SubstitutionTableNumberCodec codec = SubstitutionTableNumberCodec.lowerCaseOnly();
    private ChannelMatcher btChannelMatcher = BtChannelMatcher.create(
            Publisher.BT_TV_CHANNELS,
            ImmutableSet.of(
                    Publisher.METABROADCAST,
                    Publisher.YOUVIEW_JSON
            ));

    private Channel existingChannel;

    @Before
    public void setUp() {
        existingChannel = channelFor(
                Publisher.BT_TV_CHANNELS,
                codec.decode("bbf").longValue(),
                "dff"
        );
    }

    @Test
    public void btChannelMatcherMatchesCorrectly() throws Exception {
        Channel matchingCandidate = channelFor(
                Publisher.METABROADCAST,
                codec.decode("dff").longValue(),
                "dff"
        );

        assertTrue(btChannelMatcher.isAMatch(existingChannel, matchingCandidate));
    }

    @Test
    public void btChannelMatcherDoesNotMatchOnNonMatchingAtlasId() throws Exception {
        Channel nonMatchingCandidate = channelFor(
                Publisher.METABROADCAST,
                codec.decode("xzz").longValue(),
                "xzz"
        );

        assertFalse(btChannelMatcher.isAMatch(existingChannel, nonMatchingCandidate));
    }

    @Test (expected = IllegalArgumentException.class)
    public void channelMatcherReturnsFalseWhenMismatchedSubjectPublishers() throws Exception {
        Channel wrongPublisherChannel = channelFor(
                Publisher.BT_TV_CHANNELS_TEST1,
                123L,
                "bbd"
        );

        Channel correctCandidate = channelFor(
                Publisher.METABROADCAST,
                444L,
                "xzx"
        );

        btChannelMatcher.verifyChannelsPublishers(wrongPublisherChannel, correctCandidate);
    }

    @Test (expected = IllegalArgumentException.class)
    public void channelMatcherReturnsFalseWhenMismatchedCandidatePublishers() throws Exception {
        Channel invalidCandidate = channelFor(
                Publisher.BT,
                123L,
                "xzz"
        );

        btChannelMatcher.verifyChannelsPublishers(existingChannel, invalidCandidate);
    }

    private Channel channelFor(Publisher publisher, long thisChannelId, String baseChannelId) {
        Channel channel = Channel.builder()
                .withUri(format("http://%s/%s", publisher, baseChannelId))
                .withSource(publisher)
                .build();

        channel.setId(thisChannelId);
        return channel;
    }

}

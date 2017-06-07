package org.atlasapi.equiv.channel.matchers;

import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.entity.Publisher;
import org.junit.Test;

import static java.lang.String.format;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BtChannelMatcherTest {

    private SubstitutionTableNumberCodec codec = SubstitutionTableNumberCodec.lowerCaseOnly();
    private BtChannelMatcher btChannelMatcher = BtChannelMatcher.create(Publisher.BT_TV_CHANNELS);

    private Channel existingChannel =  channelFor(
            Publisher.BT_TV_CHANNELS,
            codec.decode("bbf").longValue(),
            "dff"
    );

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
    public void btChannelMatcherDoesNotMatchOnNonMatchingPaIds() throws Exception {
        Channel nonMatchingCandidate = channelFor(
                Publisher.METABROADCAST,
                codec.decode("xzz").longValue(),
                "xzz"
        );

        assertFalse(btChannelMatcher.isAMatch(existingChannel, nonMatchingCandidate));
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

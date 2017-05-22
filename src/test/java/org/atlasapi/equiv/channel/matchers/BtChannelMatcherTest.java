package org.atlasapi.equiv.channel.matchers;

import com.google.api.client.repackaged.com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Publisher;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BtChannelMatcherTest {

    private BtChannelMatcher btChannelMatcher = BtChannelMatcher.create();
    private Channel masterChannel;

    @Before
    public void setUp() {
        masterChannel = channelFor(Publisher.METABROADCAST, "123");
    }

    @Test
    public void btChannelMatcherMatchesCorrectly() throws Exception {
        Channel matchingAliasChannel = channelFor(Publisher.BT_TV_CHANNELS, "123");

        assertTrue(btChannelMatcher.isAMatch(masterChannel, matchingAliasChannel));
    }

    @Test
    public void btChannelMatcherDoesNotMatchOnNonMatchingPaIds() throws Exception {
        Channel nonMatchingAliasChannel = channelFor(Publisher.BT_TV_CHANNELS, "321");

        assertFalse(btChannelMatcher.isAMatch(masterChannel, nonMatchingAliasChannel));
    }

    @Test
    public void setBtChannelMatcherDoesNotMatchOnMissingPaIds() throws Exception {
        Channel missingAliasChannel = channelFor(Publisher.BT_TV_CHANNELS, "");

        assertFalse(btChannelMatcher.isAMatch(masterChannel, missingAliasChannel));
    }

    private Channel channelFor(Publisher publisher, String paId) {
        Channel channel = Channel.builder()
                .withSource(publisher)
                .withAliases(
                        Strings.isNullOrEmpty(paId) ? ImmutableSet.of()
                                                    : ImmutableSet.of(
                                                            new Alias("pa:channel:id", paId))
                )
                .build();
        channel.setId(Strings.isNullOrEmpty(paId) ? 888L : Long.parseLong(paId));

        return channel;
    }

}

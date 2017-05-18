package org.atlasapi.equiv.channel.matchers;

import com.google.api.client.repackaged.com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Publisher;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ChannelsMatcherTest {

    private BtChannelMatcher btChannelMatcher = BtChannelMatcher.create();

    @Test
    public void btChannelMatcherMatchesCorrectly() throws Exception {

        Channel masterChannel = channelFor(Publisher.METABROADCAST, "123");
        Channel matchingAliasChannel = channelFor(Publisher.BT_TV_CHANNELS, "123");
        Channel nonMatchingAliasChannel = channelFor(Publisher.BT_TV_CHANNELS, "321");
        Channel missingAliasChannel = channelFor(Publisher.BT_TV_CHANNELS, "");

        assertTrue(btChannelMatcher.isAMatch(masterChannel, matchingAliasChannel));
        assertFalse(btChannelMatcher.isAMatch(masterChannel, nonMatchingAliasChannel));
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

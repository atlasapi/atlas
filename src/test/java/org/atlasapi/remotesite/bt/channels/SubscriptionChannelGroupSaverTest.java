package org.atlasapi.remotesite.bt.channels;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;

import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelGroup;
import org.atlasapi.media.channel.ChannelGroupResolver;
import org.atlasapi.media.channel.ChannelGroupWriter;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.channel.ChannelWriter;
import org.atlasapi.media.channel.Region;
import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.MediaType;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.query.v2.ChannelGroupController;
import org.atlasapi.remotesite.bt.channels.mpxclient.Category;
import org.atlasapi.remotesite.bt.channels.mpxclient.Entry;
import org.atlasapi.remotesite.bt.channels.mpxclient.Content;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;


@RunWith( MockitoJUnitRunner.class )
public class SubscriptionChannelGroupSaverTest {
    
    private static final long CHANNEL_GROUP_123456_ID = 1;
    private static final String CHANNEL1_KEY = "kdcv";
    private static final String CHANNEL2_KEY = "hqcs";
    private static final Channel CHANNEL1 = new Channel(Publisher.METABROADCAST, "Channel 1", "a", true, MediaType.VIDEO, "http://channel1.com");
    private static final Channel CHANNEL2 = new Channel(Publisher.METABROADCAST, "Channel 2", "b", true, MediaType.VIDEO, "http://channel2.com");
    private static final String ALIAS_URI_PREFIX = "http://example.org/";
    private static final String ALIAS_NAMESPACE = "a:namespace";
    
    private final ChannelGroupResolver channelGroupResolver = mock(ChannelGroupResolver.class);
    private final ChannelGroupWriter channelGroupWriter = mock(ChannelGroupWriter.class);
    private final ChannelResolver channelResolver = mock(ChannelResolver.class);
    private final ChannelWriter channelWriter = mock(ChannelWriter.class);
    
    private final NumberToShortStringCodec codec = SubstitutionTableNumberCodec.lowerCaseOnly();
    private final Long channel1Id = codec.decode(CHANNEL1_KEY).longValue();
    private final Long channel2Id = codec.decode(CHANNEL2_KEY).longValue();
    private final SubscriptionChannelGroupSaver saver 
        = new SubscriptionChannelGroupSaver(Publisher.METABROADCAST, ALIAS_URI_PREFIX, ALIAS_NAMESPACE, 
                channelGroupResolver, channelGroupWriter, channelResolver, channelWriter);
    
    
    @Before
    public void setUp() {
        CHANNEL1.setId(channel1Id);
        CHANNEL2.setId(channel2Id);
    }
    
    @Test
    public void testExtractsSubscriptions() {
        String theirId1 = "S0123456";
        String theirId2 = "S6543210";
        
        when(channelResolver.forIds(ImmutableSet.<Long>of(channel1Id)))
            .thenReturn(ImmutableSet.of(CHANNEL1));
        when(channelResolver.forIds(ImmutableSet.<Long>of(channel2Id)))
            .thenReturn(ImmutableSet.of(CHANNEL2));
        when(channelGroupResolver.channelGroupFor(ALIAS_URI_PREFIX + theirId1))
            .thenReturn(Optional.<ChannelGroup>of(channelGroup(theirId1, CHANNEL_GROUP_123456_ID)));
        when(channelGroupResolver.channelGroupFor(ALIAS_URI_PREFIX + theirId2))
            .thenReturn(Optional.<ChannelGroup>of(channelGroup(theirId2, 2)));
        saver.update(ImmutableList.of(channelWithSubscription(CHANNEL1_KEY, "S0123456"),
                channelWithSubscription(CHANNEL2_KEY, "S6543210")));
        
        ArgumentCaptor<Channel> channelCaptor = ArgumentCaptor.forClass(Channel.class);
        verify(channelWriter, times(2)).createOrUpdate(channelCaptor.capture());

        Map<Long, Channel> map = Maps.uniqueIndex(channelCaptor.getAllValues(), new Function<Channel, Long>() {

            @Override
            public Long apply(Channel input) {
                return input.getId();
            }
            
        });
        Channel channel = map.get(channel1Id);
        assertThat(Iterables.getOnlyElement(channel.getChannelNumbers()).getChannelGroup(), 
                is(CHANNEL_GROUP_123456_ID));
    }
    
    public Entry channelWithSubscription(String channelId, String subscriptionId) {
        Category category = new Category(subscriptionId, "subscription", "");
        return new Entry(channelId, 0, "Title", 
                    ImmutableList.of(category), 
                    ImmutableList.<Content>of(), 
                    true, null, null, true, false, 0, "urn:BT:linear:service:751764");
    }
    
    private ChannelGroup channelGroup(String remoteId, long atlasId) {
        ChannelGroup group = new Region();
        group.setCanonicalUri(ALIAS_URI_PREFIX + remoteId);
        group.setId(atlasId);
        group.setAliases(ImmutableSet.of(new Alias(ALIAS_NAMESPACE, remoteId)));
        return group;
    }
}

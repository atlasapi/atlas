package org.atlasapi.remotesite.bt.channels;

import static org.hamcrest.Matchers.equalTo;
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
import org.atlasapi.media.channel.ChannelNumbering;
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
public class AllBtChannelsChannelGroupSaverTest {
    
    private static final long CHANNEL_GROUP_123456_ID = 1;
    private static final String CHANNEL1_KEY = "kdcv";
    private static final Channel CHANNEL1 = new Channel(Publisher.METABROADCAST, "Channel 1", "a", true, MediaType.VIDEO, "http://channel1.com");
    private static final String ALIAS_URI_PREFIX = "http://example.org/";
    private static final String ALIAS_NAMESPACE = "a:namespace";
    
    private final ChannelGroupResolver channelGroupResolver = mock(ChannelGroupResolver.class);
    private final ChannelGroupWriter channelGroupWriter = mock(ChannelGroupWriter.class);
    private final ChannelResolver channelResolver = mock(ChannelResolver.class);
    private final ChannelWriter channelWriter = mock(ChannelWriter.class);
    
    private final NumberToShortStringCodec codec = SubstitutionTableNumberCodec.lowerCaseOnly();
    private final Long channel1Id = codec.decode(CHANNEL1_KEY).longValue();
    private final AllBtChannelsChannelGroupSaver saver 
        = new AllBtChannelsChannelGroupSaver(Publisher.METABROADCAST, ALIAS_URI_PREFIX, ALIAS_NAMESPACE, 
                channelGroupResolver, channelGroupWriter, channelResolver, channelWriter);
    
    
    @Before
    public void setUp() {
        CHANNEL1.setId(channel1Id);
    }
    
    @Test
    public void testExtractsChannelNumber() {
        
        when(channelResolver.forIds(ImmutableSet.<Long>of(channel1Id)))
            .thenReturn(ImmutableSet.of(CHANNEL1));
        when(channelGroupResolver.channelGroupFor(ALIAS_URI_PREFIX + "bt-channels"))
            .thenReturn(Optional.of(channelGroup("bt-channels", 1234)));
        saver.update(ImmutableList.of(channelWithLcn(CHANNEL1_KEY, "S0123456")));
        
        ArgumentCaptor<Channel> channelCaptor = ArgumentCaptor.forClass(Channel.class);
        verify(channelWriter, times(1)).createOrUpdate(channelCaptor.capture());

        Channel savedChannel = channelCaptor.getValue();
        ChannelNumbering numbering = Iterables.getOnlyElement(savedChannel.getChannelNumbers());
        assertThat(numbering.getChannelNumber(), is(equalTo("101")));
    }
    
    public Entry channelWithLcn(String channelId, String subscriptionId) {
        return new Entry(channelId, 0, "Title", 
                    ImmutableList.<Category>of(), 
                    ImmutableList.<Content>of(), 
                    true, null, null, true, false, "101");
    }
    
    private ChannelGroup channelGroup(String remoteId, long atlasId) {
        ChannelGroup group = new Region();
        group.setCanonicalUri(ALIAS_URI_PREFIX + remoteId);
        group.setId(atlasId);
        group.setAliases(ImmutableSet.of(new Alias(ALIAS_NAMESPACE, remoteId)));
        return group;
    }
}

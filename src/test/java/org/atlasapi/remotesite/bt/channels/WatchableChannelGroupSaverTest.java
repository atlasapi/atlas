package org.atlasapi.remotesite.bt.channels;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelGroup;
import org.atlasapi.media.channel.ChannelGroupResolver;
import org.atlasapi.media.channel.ChannelGroupWriter;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.channel.ChannelWriter;
import org.atlasapi.media.channel.Region;
import org.atlasapi.media.entity.MediaType;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.remotesite.bt.channels.mpxclient.BtMpxClientException;
import org.atlasapi.remotesite.bt.channels.mpxclient.Category;
import org.atlasapi.remotesite.bt.channels.mpxclient.Content;
import org.atlasapi.remotesite.bt.channels.mpxclient.Entry;
import org.joda.time.DateTime;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;


@RunWith( MockitoJUnitRunner.class )
public class WatchableChannelGroupSaverTest {
    
    private static final long CHANNEL_GROUP_ID = 1L;
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
    
    private final WatchableChannelGroupSaver saver 
        = new WatchableChannelGroupSaver(Publisher.METABROADCAST, ALIAS_URI_PREFIX, ALIAS_NAMESPACE, 
                channelGroupResolver, channelGroupWriter, channelResolver, channelWriter);
    
    
    @Test
    public void testsExtractsWatchableChannelGroup() throws BtMpxClientException {
        
        when(channelGroupResolver.channelGroupFor(ALIAS_URI_PREFIX + "watchables"))
            .thenReturn(Optional.<ChannelGroup>of(watchableChannelGroup()));
        when(channelResolver.forIds(ImmutableSet.<Long>of(channel1Id)))
            .thenReturn(ImmutableSet.of(CHANNEL1));
        when(channelResolver.forIds(ImmutableSet.<Long>of(channel2Id)))
            .thenReturn(ImmutableSet.of(CHANNEL2));
        
        saver.update(ImmutableList.of(watchableChannel(CHANNEL1_KEY, true),
                watchableChannel(CHANNEL2_KEY, false)));
        
        ArgumentCaptor<Channel> channelCaptor = ArgumentCaptor.forClass(Channel.class);
        verify(channelWriter).createOrUpdate(channelCaptor.capture());

        Channel channel = Iterables.getOnlyElement(channelCaptor.getAllValues());
        
        assertThat(Iterables.getOnlyElement(channel.getChannelNumbers()).getChannelGroup(), is(CHANNEL_GROUP_ID));
    }

    public Entry watchableChannel(String channelId, boolean isWatchable) {
        return new Entry(channelId, 0, "Title", 
                    ImmutableList.<Category>of(), 
                    ImmutableList.<Content>of(), 
                    true, null, null, isWatchable, false, DateTime.now(), "urn:BT:linear:service:751764");
    }
    
    private ChannelGroup watchableChannelGroup() {
        ChannelGroup group = new Region();
        group.setCanonicalUri(ALIAS_URI_PREFIX + "watchables");
        group.setId(CHANNEL_GROUP_ID);
        return group;
    }
}

package org.atlasapi.query.v2;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.atlasapi.media.channel.ChannelGroupStore;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.channel.ChannelStore;
import org.atlasapi.media.channel.Platform;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.simple.Channel;
import org.atlasapi.media.entity.simple.ChannelGroup;
import org.atlasapi.media.entity.simple.ChannelNumbering;

import com.metabroadcast.common.base.Maybe;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ChannelGroupWriteExecutorTest {

    private Channel simpleChannel;
    private org.atlasapi.media.channel.Channel complexChannel;
    private ChannelResolver channelResolver;
    private ChannelStore channelStore;
    private ChannelNumbering channelNumbering;
    private ChannelGroup simpleChannelGroup;
    private org.atlasapi.media.channel.ChannelGroup complexChannelGroup;
    private org.atlasapi.media.channel.ChannelNumbering complexChannelNumbering;
    private ChannelGroupStore channelGroupStore;
    private HttpServletRequest request;
    private HttpServletResponse response;

    private ChannelGroupWriteExecutor executor;

    @Before
    public void setUp() {
        simpleChannel = mock(Channel.class);
        complexChannel = mock(org.atlasapi.media.channel.Channel.class);
        channelResolver = mock(ChannelResolver.class);
        channelStore = mock(ChannelStore.class);
        channelNumbering = mock(ChannelNumbering.class);
        complexChannelGroup = mock(org.atlasapi.media.channel.ChannelGroup.class);
        complexChannelNumbering = mock(org.atlasapi.media.channel.ChannelNumbering.class);
        simpleChannelGroup = mock(ChannelGroup.class);
        channelGroupStore = mock(ChannelGroupStore.class);
        request = mock(HttpServletRequest.class);
        response = mock(HttpServletResponse.class);

        executor = ChannelGroupWriteExecutor.create(channelGroupStore, channelStore);
    }

    @Test
    public void testUpdateExistingChannelGroup() {
        when(complexChannelGroup.getId()).thenReturn(17L);
        when(channelGroupStore.channelGroupFor(17L)).thenReturn(Optional.of(complexChannelGroup));
        when(channelGroupStore.createOrUpdate(complexChannelGroup)).thenReturn(complexChannelGroup);
        when(complexChannelGroup.getChannelNumberings()).thenReturn(Sets.newHashSet(complexChannelNumbering));
        when(complexChannelNumbering.getChannel()).thenReturn(16L);
        when(channelResolver.fromId(anyLong())).thenReturn(Maybe.just(complexChannel));
        when(simpleChannelGroup.getChannels()).thenReturn(Lists.newArrayList(channelNumbering));
        when(channelNumbering.getChannel()).thenReturn(simpleChannel);
        when(simpleChannel.getId()).thenReturn("pnd");

        executor.createOrUpdateChannelGroup(
                request,
                complexChannelGroup,
                simpleChannelGroup,
                channelResolver
        );

        verify(channelGroupStore, times(1)).channelGroupFor(17L);
        verify(channelGroupStore, times(1)).createOrUpdate(complexChannelGroup);
        verify(channelStore, times(2)).createOrUpdate(complexChannel);
    }

    @Test
    public void testWriteNewChannelGroupAndUpdateChannelNumberings() {
        org.atlasapi.media.channel.ChannelGroup updatedChannelGroup = new Platform();
        updatedChannelGroup.setId(17L);
        updatedChannelGroup.setPublisher(Publisher.BT_TV_CHANNELS);

        when(complexChannelGroup.getId()).thenReturn(null);
        when(channelGroupStore.createOrUpdate(complexChannelGroup)).thenReturn(updatedChannelGroup);
        when(channelGroupStore.createOrUpdate(updatedChannelGroup)).thenReturn(updatedChannelGroup);
        when(simpleChannelGroup.getChannels()).thenReturn(Lists.newArrayList(channelNumbering));
        when(channelNumbering.getChannel()).thenReturn(simpleChannel);
        when(simpleChannel.getId()).thenReturn("pnd");
        when(channelResolver.fromId(anyLong())).thenReturn(Maybe.just(complexChannel));

        executor.createOrUpdateChannelGroup(
                request,
                complexChannelGroup,
                simpleChannelGroup,
                channelResolver
        );

        verify(channelGroupStore, times(1)).createOrUpdate(complexChannelGroup);
        verify(channelGroupStore, times(1)).createOrUpdate(updatedChannelGroup);
        verify(channelStore, times(1)).createOrUpdate(complexChannel);
    }

    @Test
    public void testDeleteExistingChannelGroup() {
        when(channelGroupStore.channelGroupFor(17L)).thenReturn(Optional.of(complexChannelGroup));
        when(complexChannelGroup.getChannelNumberings()).thenReturn(Sets.newHashSet(complexChannelNumbering));
        when(complexChannelNumbering.getChannel()).thenReturn(16L);
        when(channelResolver.fromId(16L)).thenReturn(Maybe.just(complexChannel));

        executor.deletePlatform(request, response, 17L, channelResolver);

        verify(channelGroupStore, times(1)).channelGroupFor(17L);
        verify(channelStore, times(1)).createOrUpdate(complexChannel);
        verify(channelGroupStore, times(1)).deleteChannelGroupById(17L);
    }

}

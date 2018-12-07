package org.atlasapi.query.v2;

import java.util.Date;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.atlasapi.media.channel.ChannelGroupStore;
import org.atlasapi.media.channel.Platform;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.simple.Channel;
import org.atlasapi.media.entity.simple.ChannelGroup;
import org.atlasapi.media.entity.simple.ChannelNumbering;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ChannelGroupWriteExecutorTest {

    private Channel channel;
    private ChannelNumbering channelNumbering;
    private ChannelGroup simpleChannelGroup;
    private org.atlasapi.media.channel.ChannelGroup complexChannelGroup;
    private org.atlasapi.media.channel.ChannelGroup existingComplexChannelGroup;
    private ChannelGroupStore store;
    private HttpServletRequest request;
    private HttpServletResponse response;

    private ChannelGroupWriteExecutor executor;

    @Before
    public void setUp() {
        channel = mock(Channel.class);
        channelNumbering = mock(ChannelNumbering.class);
        complexChannelGroup = mock(org.atlasapi.media.channel.ChannelGroup.class);
        simpleChannelGroup = mock(ChannelGroup.class);
        store = mock(ChannelGroupStore.class);
        request = mock(HttpServletRequest.class);
        response = mock(HttpServletResponse.class);

        executor = ChannelGroupWriteExecutor.create(store);
    }

    @Test
    public void testUpdateExistingChannelGroupLogic() {
        when(complexChannelGroup.getId()).thenReturn(17L);

        executor.createOrUpdateChannelGroup(request, complexChannelGroup, simpleChannelGroup);

        verify(store, times(1)).createOrUpdate(complexChannelGroup);
    }

    @Test
    public void testWriteNewChanneLGroupAndUpdateChannelNumberings() {
        existingComplexChannelGroup = new Platform();
        existingComplexChannelGroup.setId(17L);
        existingComplexChannelGroup.setPublisher(Publisher.BT_TV_CHANNELS);

        when(complexChannelGroup.getId()).thenReturn(null);
        when(store.createOrUpdate(complexChannelGroup)).thenReturn(existingComplexChannelGroup);
        when(store.channelGroupFor(17L)).thenReturn(Optional.of(existingComplexChannelGroup));
        when(simpleChannelGroup.getChannels()).thenReturn(ImmutableList.of(channelNumbering));
        when(channelNumbering.getChannel()).thenReturn(channel);
        when(channelNumbering.getStartDate()).thenReturn(new Date());
        when(channelNumbering.getEndDate()).thenReturn(new Date());
        when(channel.getId()).thenReturn("bc");

        Assert.assertTrue(complexChannelGroup.getChannelNumberings().isEmpty());

        executor.createOrUpdateChannelGroup(request, complexChannelGroup, simpleChannelGroup);

        Assert.assertFalse(existingComplexChannelGroup.getChannelNumberings().isEmpty());

        org.atlasapi.media.channel.ChannelNumbering channelNumbering = existingComplexChannelGroup.getChannelNumberings().iterator().next();
        Assert.assertTrue(channelNumbering.getChannel() == 1L);
        Assert.assertTrue(channelNumbering.getChannelGroup() == 17L);

        verify(store, times(1)).createOrUpdate(complexChannelGroup);
        verify(store, times(1)).createOrUpdate(existingComplexChannelGroup);
        verify(store, times(1)).channelGroupFor(17L);
    }

    @Test
    public void testDeleteExistingChannelGroupLogic() {
        executor.deletePlatform(request, response, 17L);

        verify(store, times(1)).deleteChannelGroupById(17L);
    }

}

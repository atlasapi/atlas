package org.atlasapi.query.v2;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.metabroadcast.common.base.Maybe;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelGroup;
import org.atlasapi.media.channel.ChannelGroupStore;
import org.atlasapi.media.channel.ChannelNumbering;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.channel.ChannelStore;
import org.atlasapi.media.channel.Platform;
import org.joda.time.LocalDate;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ChannelGroupWriteExecutorTest {

    private ChannelResolver channelResolver;
    private ChannelStore channelStore;
    private ChannelGroupStore channelGroupStore;
    private HttpServletRequest request;
    private HttpServletResponse response;

    private ChannelGroupWriteExecutor executor;

    @Before
    public void setUp() {
        channelResolver = mock(ChannelResolver.class);
        channelStore = mock(ChannelStore.class);
        channelGroupStore = mock(ChannelGroupStore.class);
        request = mock(HttpServletRequest.class);
        response = mock(HttpServletResponse.class);
        executor = ChannelGroupWriteExecutor.create(channelGroupStore, channelStore);
    }

    private org.atlasapi.media.entity.simple.ChannelNumbering simpleNumbering(String channelId, String channelNumber) {
        return simpleNumbering(channelId, channelNumber, null, null);
    }

    private org.atlasapi.media.entity.simple.ChannelNumbering simpleNumbering(
            String channelId,
            String channelNumber,
            @Nullable LocalDate start,
            @Nullable LocalDate end
    ) {
        org.atlasapi.media.entity.simple.ChannelNumbering numbering = new org.atlasapi.media.entity.simple.ChannelNumbering();
        org.atlasapi.media.entity.simple.Channel channel = new org.atlasapi.media.entity.simple.Channel();
        channel.setId(channelId);
        numbering.setChannel(channel);
        numbering.setChannelNumber(channelNumber);
        numbering.setStartDate(start.toDate());
        numbering.setEndDate(end.toDate());
        return numbering;
    }

    @Test
    public void testChannelNumberIsUpdated() {
        ChannelGroup existingChannelGroup = new Platform();
        existingChannelGroup.setId(1L);
        ChannelNumbering existingChannelNumbering = ChannelNumbering.builder()
                .withChannel(10L)
                .withChannelGroup(existingChannelGroup.getId())
                .withChannelNumber("10")
                .build();
        existingChannelGroup.addChannelNumbering(existingChannelNumbering);
        Channel channel = Channel.builder()
                .withChannelNumber(existingChannelNumbering)
                .build();
        channel.setId(10L);

        when(channelGroupStore.channelGroupFor(1L)).thenReturn(Optional.of(existingChannelGroup));
        when(channelResolver.fromId(10L)).thenReturn(Maybe.just(channel));

        ChannelGroup newChannelGroup = new Platform();
        newChannelGroup.setId(1L);

        when(channelGroupStore.createOrUpdate(newChannelGroup)).thenReturn(newChannelGroup);

        executor.createOrUpdateChannelGroup(
                request,
                newChannelGroup,
                ImmutableList.of(
                        simpleNumbering("p", "20")
                ),
                channelResolver
        );

        Set<ChannelNumbering> expectedNumberings = ImmutableSet.of(
                ChannelNumbering.builder()
                        .withChannel(10L)
                        .withChannelGroup(existingChannelGroup.getId())
                        .withChannelNumber("20")
                        .build()
        );

        assertEquals(expectedNumberings, newChannelGroup.getChannelNumberings());
        verify(channelGroupStore, times(1)).createOrUpdate(newChannelGroup);
        assertEquals(expectedNumberings, channel.getChannelNumbers());
        verify(channelStore, times(1)).createOrUpdate(channel);
    }

    @Test
    public void testNumberingOnNewChannelIsAdded() {
        ChannelGroup existingChannelGroup = new Platform();
        existingChannelGroup.setId(1L);
        Channel channel = Channel.builder().build();
        channel.setId(10L);

        when(channelGroupStore.channelGroupFor(1L)).thenReturn(Optional.of(existingChannelGroup));
        when(channelResolver.fromId(10L)).thenReturn(Maybe.just(channel));

        ChannelGroup newChannelGroup = new Platform();
        newChannelGroup.setId(1L);

        when(channelGroupStore.createOrUpdate(newChannelGroup)).thenReturn(newChannelGroup);

        executor.createOrUpdateChannelGroup(
                request,
                newChannelGroup,
                ImmutableList.of(
                        simpleNumbering("p", "20")
                ),
                channelResolver
        );

        Set<ChannelNumbering> expectedNumberings = ImmutableSet.of(
                ChannelNumbering.builder()
                        .withChannel(10L)
                        .withChannelGroup(existingChannelGroup.getId())
                        .withChannelNumber("20")
                        .build()
        );

        assertEquals(expectedNumberings, newChannelGroup.getChannelNumberings());
        verify(channelGroupStore, times(1)).createOrUpdate(newChannelGroup);
        assertEquals(expectedNumberings, channel.getChannelNumbers());
        verify(channelStore, times(1)).createOrUpdate(channel);
    }

    @Test
    public void testNumberingOnRemovedChannelIsRemoved() {
        ChannelGroup existingChannelGroup = new Platform();
        existingChannelGroup.setId(1L);
        ChannelNumbering existingChannelNumbering = ChannelNumbering.builder()
                .withChannel(10L)
                .withChannelGroup(existingChannelGroup.getId())
                .withChannelNumber("10")
                .build();
        existingChannelGroup.addChannelNumbering(existingChannelNumbering);
        Channel channel = Channel.builder()
                .withChannelNumber(existingChannelNumbering)
                .build();
        channel.setId(10L);

        when(channelGroupStore.channelGroupFor(1L)).thenReturn(Optional.of(existingChannelGroup));
        when(channelResolver.fromId(10L)).thenReturn(Maybe.just(channel));

        ChannelGroup newChannelGroup = new Platform();
        newChannelGroup.setId(1L);

        when(channelGroupStore.createOrUpdate(newChannelGroup)).thenReturn(newChannelGroup);

        executor.createOrUpdateChannelGroup(
                request,
                newChannelGroup,
                ImmutableList.of(),
                channelResolver
        );

        Set<ChannelNumbering> expectedNumberings = ImmutableSet.of();

        assertEquals(expectedNumberings, newChannelGroup.getChannelNumberings());
        verify(channelGroupStore, times(1)).createOrUpdate(newChannelGroup);
        assertEquals(expectedNumberings, channel.getChannelNumbers());
        verify(channelStore, times(1)).createOrUpdate(channel);
    }

    @Test
    public void testNoChangeDoesNotUpdateChannel() {
        ChannelGroup existingChannelGroup = new Platform();
        existingChannelGroup.setId(1L);
        ChannelNumbering existingChannelNumbering = ChannelNumbering.builder()
                .withChannel(10L)
                .withChannelGroup(existingChannelGroup.getId())
                .withChannelNumber("10")
                .build();
        existingChannelGroup.addChannelNumbering(existingChannelNumbering);
        Channel channel = Channel.builder()
                .withChannelNumber(existingChannelNumbering)
                .build();
        channel.setId(10L);

        when(channelGroupStore.channelGroupFor(1L)).thenReturn(Optional.of(existingChannelGroup));
        when(channelResolver.fromId(10L)).thenReturn(Maybe.just(channel));

        ChannelGroup newChannelGroup = new Platform();
        newChannelGroup.setId(1L);

        when(channelGroupStore.createOrUpdate(newChannelGroup)).thenReturn(newChannelGroup);

        executor.createOrUpdateChannelGroup(
                request,
                newChannelGroup,
                ImmutableList.of(
                        simpleNumbering("p", "10")
                ),
                channelResolver
        );

        Set<ChannelNumbering> expectedNumberings = ImmutableSet.of(
                ChannelNumbering.builder()
                        .withChannel(10L)
                        .withChannelGroup(existingChannelGroup.getId())
                        .withChannelNumber("10")
                        .build()
        );

        assertEquals(expectedNumberings, newChannelGroup.getChannelNumberings());
        verify(channelGroupStore, times(1)).createOrUpdate(newChannelGroup);
        assertEquals(expectedNumberings, channel.getChannelNumbers());
        verify(channelStore, times(0)).createOrUpdate(channel);
    }

    @Test
    public void testOnlyOldNumberingsForChannelGroupRemovedFromChannel() {
        ChannelGroup existingChannelGroup = new Platform();
        existingChannelGroup.setId(1L);
        Set<ChannelNumbering> existingChannelNumberings = ImmutableSet.of(
                ChannelNumbering.builder()
                        .withChannel(10L)
                        .withChannelGroup(existingChannelGroup.getId())
                        .withChannelNumber("10")
                        .withStartDate(LocalDate.parse("2020-01-01"))
                        .withEndDate(LocalDate.parse("2030-01-01"))
                        .build(),
                ChannelNumbering.builder()
                        .withChannel(10L)
                        .withChannelGroup(existingChannelGroup.getId())
                        .withChannelNumber("100")
                        .withStartDate(LocalDate.parse("2010-01-01"))
                        .withEndDate(LocalDate.parse("2020-01-01"))
                        .build()
        );
        existingChannelGroup.setChannelNumberings(existingChannelNumberings);

        Set<ChannelNumbering> numberingsForOtherChannelGroups = ImmutableSet.of(
                ChannelNumbering.builder()
                        .withChannel(10L)
                        .withChannelGroup(2L)
                        .withChannelNumber("10")
                        .withStartDate(LocalDate.parse("2020-01-01"))
                        .withEndDate(LocalDate.parse("2030-01-01"))
                        .build(),
                ChannelNumbering.builder()
                        .withChannel(10L)
                        .withChannelGroup(3L)
                        .withChannelNumber("100")
                        .withStartDate(LocalDate.parse("2010-01-01"))
                        .withEndDate(LocalDate.parse("2020-01-01"))
                        .build()
        );
        Channel channel = Channel.builder()
                .withChannelNumbers(Sets.union(numberingsForOtherChannelGroups, existingChannelNumberings))
                .build();
        channel.setId(10L);

        when(channelGroupStore.channelGroupFor(1L)).thenReturn(Optional.of(existingChannelGroup));
        when(channelResolver.fromId(10L)).thenReturn(Maybe.just(channel));

        ChannelGroup newChannelGroup = new Platform();
        newChannelGroup.setId(1L);

        when(channelGroupStore.createOrUpdate(newChannelGroup)).thenReturn(newChannelGroup);

        executor.createOrUpdateChannelGroup(
                request,
                newChannelGroup,
                ImmutableList.of(
                        simpleNumbering("p", "20", LocalDate.parse("2000-06-01"), LocalDate.parse("2005-12-01")),
                        simpleNumbering("p", "21", LocalDate.parse("2020-06-01"), LocalDate.parse("2025-12-01"))
                ),
                channelResolver
        );

        Set<ChannelNumbering> expectedChannelGroupNumberings = ImmutableSet.of(
                ChannelNumbering.builder()
                        .withChannel(10L)
                        .withChannelGroup(existingChannelGroup.getId())
                        .withChannelNumber("20")
                        .withStartDate(LocalDate.parse("2000-06-01"))
                        .withEndDate(LocalDate.parse("2005-12-01"))
                        .build(),
                ChannelNumbering.builder()
                        .withChannel(10L)
                        .withChannelGroup(existingChannelGroup.getId())
                        .withChannelNumber("21")
                        .withStartDate(LocalDate.parse("2020-06-01"))
                        .withEndDate(LocalDate.parse("2025-12-01"))
                        .build()
        );

        assertEquals(expectedChannelGroupNumberings, newChannelGroup.getChannelNumberings());
        verify(channelGroupStore, times(1)).createOrUpdate(newChannelGroup);
        assertEquals(Sets.union(numberingsForOtherChannelGroups, expectedChannelGroupNumberings), channel.getChannelNumbers());
        verify(channelStore, times(1)).createOrUpdate(channel);
    }

    @Test
    public void testMultipleUpdateTypesOnMultipleChannels() {
        ChannelGroup existingChannelGroup = new Platform();
        existingChannelGroup.setId(1L);
        ChannelNumbering existingChannelNumbering1 = ChannelNumbering.builder()
                .withChannel(10L)
                .withChannelGroup(existingChannelGroup.getId())
                .withChannelNumber("10")
                .build();
        existingChannelGroup.addChannelNumbering(existingChannelNumbering1);
        ChannelNumbering existingChannelNumbering2 = ChannelNumbering.builder()
                .withChannel(11L)
                .withChannelGroup(existingChannelGroup.getId())
                .withChannelNumber("11")
                .build();
        existingChannelGroup.addChannelNumbering(existingChannelNumbering2);
        ChannelNumbering existingChannelNumbering3 = ChannelNumbering.builder()
                .withChannel(13L)
                .withChannelGroup(existingChannelGroup.getId())
                .withChannelNumber("13")
                .build();
        existingChannelGroup.addChannelNumbering(existingChannelNumbering3);
        Channel channel1 = Channel.builder()
                .withChannelNumber(existingChannelNumbering1)
                .build();
        channel1.setId(10L);
        Channel channel2 = Channel.builder()
                .withChannelNumber(existingChannelNumbering2)
                .build();
        channel2.setId(11L);
        Channel channel3 = Channel.builder()
                .build();
        channel3.setId(12L);
        Channel channel4 = Channel.builder()
                .withChannelNumber(existingChannelNumbering3)
                .build();
        channel4.setId(13L);

        when(channelGroupStore.channelGroupFor(1L)).thenReturn(Optional.of(existingChannelGroup));
        when(channelResolver.fromId(10L)).thenReturn(Maybe.just(channel1));
        when(channelResolver.fromId(11L)).thenReturn(Maybe.just(channel2));
        when(channelResolver.fromId(12L)).thenReturn(Maybe.just(channel3));
        when(channelResolver.fromId(13L)).thenReturn(Maybe.just(channel4));

        ChannelGroup newChannelGroup = new Platform();
        newChannelGroup.setId(1L);

        when(channelGroupStore.createOrUpdate(newChannelGroup)).thenReturn(newChannelGroup);

        executor.createOrUpdateChannelGroup(
                request,
                newChannelGroup,
                ImmutableList.of(
                        simpleNumbering("p", "20"),
                        simpleNumbering("r", "22"),
                        simpleNumbering("s", "13")
                ),
                channelResolver
        );

        ChannelNumbering expectedChannelNumbering1 = ChannelNumbering.builder()
                .withChannel(10L)
                .withChannelGroup(existingChannelGroup.getId())
                .withChannelNumber("20")
                .build();

        ChannelNumbering expectedChannelNumbering2 = ChannelNumbering.builder()
                .withChannel(12L)
                .withChannelGroup(existingChannelGroup.getId())
                .withChannelNumber("22")
                .build();

        ChannelNumbering expectedChannelNumbering3 = ChannelNumbering.builder()
                .withChannel(13L)
                .withChannelGroup(existingChannelGroup.getId())
                .withChannelNumber("13")
                .build();

        Set<ChannelNumbering> expectedNumberings = ImmutableSet.of(
                expectedChannelNumbering1,
                expectedChannelNumbering2,
                expectedChannelNumbering3
        );

        assertEquals(expectedNumberings, newChannelGroup.getChannelNumberings());
        verify(channelGroupStore, times(1)).createOrUpdate(newChannelGroup);

        //Modify
        assertEquals(ImmutableSet.of(expectedChannelNumbering1), channel1.getChannelNumbers());
        verify(channelStore, times(1)).createOrUpdate(channel1);

        //Remove
        assertEquals(ImmutableSet.of(), channel2.getChannelNumbers());
        verify(channelStore, times(1)).createOrUpdate(channel2);

        //Add
        assertEquals(ImmutableSet.of(expectedChannelNumbering2), channel3.getChannelNumbers());
        verify(channelStore, times(1)).createOrUpdate(channel3);

        //Unchanged
        assertEquals(ImmutableSet.of(expectedChannelNumbering3), channel4.getChannelNumbers());
        verify(channelStore, times(0)).createOrUpdate(channel4);
    }

    @Test
    public void testDeletePlatformUpdatesAllChannels() {
        ChannelGroup existingChannelGroup = new Platform();
        existingChannelGroup.setId(1L);
        ChannelNumbering existingChannelNumbering1 = ChannelNumbering.builder()
                .withChannel(10L)
                .withChannelGroup(existingChannelGroup.getId())
                .withChannelNumber("10")
                .withStartDate(LocalDate.parse("2020-01-01"))
                .withEndDate(LocalDate.parse("2030-01-01"))
                .build();
        existingChannelGroup.addChannelNumbering(existingChannelNumbering1);
        ChannelNumbering existingChannelNumbering2 = ChannelNumbering.builder()
                .withChannel(11L)
                .withChannelGroup(existingChannelGroup.getId())
                .withChannelNumber("100")
                .withStartDate(LocalDate.parse("2010-01-01"))
                .withEndDate(LocalDate.parse("2020-01-01"))
                .build();
        existingChannelGroup.addChannelNumbering(existingChannelNumbering2);

        Channel channel1 = Channel.builder()
                .withChannelNumber(existingChannelNumbering1)
                .build();
        channel1.setId(10L);
        Channel channel2 = Channel.builder()
                .withChannelNumber(existingChannelNumbering2)
                .build();
        channel2.setId(11L);

        when(channelGroupStore.channelGroupFor(1L)).thenReturn(Optional.of(existingChannelGroup));
        when(channelResolver.fromId(10L)).thenReturn(Maybe.just(channel1));
        when(channelResolver.fromId(11L)).thenReturn(Maybe.just(channel2));

        executor.deletePlatform(
                request,
                response,
                1L,
                channelResolver
        );

        verify(channelGroupStore, times(1)).deleteChannelGroupById(1L);
        assertEquals(ImmutableSet.of(), channel1.getChannelNumbers());
        verify(channelStore, times(1)).createOrUpdate(channel1);
        assertEquals(ImmutableSet.of(), channel2.getChannelNumbers());
        verify(channelStore, times(1)).createOrUpdate(channel2);
    }
}

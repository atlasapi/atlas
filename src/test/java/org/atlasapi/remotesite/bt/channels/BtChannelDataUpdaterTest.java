package org.atlasapi.remotesite.bt.channels;

import com.google.api.client.util.Lists;
import com.google.common.collect.ImmutableList;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.channel.ChannelWriter;
import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.MediaType;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.remotesite.bt.channels.mpxclient.Category;
import org.atlasapi.remotesite.bt.channels.mpxclient.Content;
import org.atlasapi.remotesite.bt.channels.mpxclient.Entry;
import org.atlasapi.remotesite.bt.channels.mpxclient.PaginatedEntries;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class BtChannelDataUpdaterTest {
    private BtChannelDataUpdater channelDataUpdater;
    private PaginatedEntries paginatedEntries;
    private ChannelResolver channelResolver;
    private ChannelWriter channelWriter;

    private final SubstitutionTableNumberCodec codec = new SubstitutionTableNumberCodec();

    private static final Channel CHANNEL1 = new Channel(Publisher.METABROADCAST, "Channel 1", "a", true, MediaType.VIDEO, "http://channel1.com");
    private static final Channel EXPECTED_CHANNEL_WITH_ALIAS = new Channel(Publisher.METABROADCAST, "Channel 2", "b", true, MediaType.VIDEO, "http://channel2.com");

    private static final String ALIAS_NAMESPACE = "namespace";
    private static final String LINEAR_CHANNEL_ID = "urn:BT:linear:service:751764";
    @Before
    public void setUp() throws Exception {
        channelResolver = mock(ChannelResolver.class);
        channelWriter = mock(ChannelWriter.class);
        channelDataUpdater = new BtChannelDataUpdater(channelResolver, channelWriter, ALIAS_NAMESPACE);
    }

    @Test
    public void testAddAliasesToChannel() throws Exception {
        paginatedEntries = mock(PaginatedEntries.class);

        List<Entry> entries = Lists.newArrayList();
        Entry entry1 = entryForTesting();
        entries.add(entry1);

        List<Channel> channels = Lists.newArrayList();
        channels.add(CHANNEL1);
        Channel expectedChannelWithAlias = new Channel(Publisher.METABROADCAST, "Channel 1", "a", true, MediaType.VIDEO, "http://channel1.com");

        expectedChannelWithAlias.addAlias(new Alias(ALIAS_NAMESPACE, LINEAR_CHANNEL_ID));

        long currentGuid = codec.decode(entry1.getGuid()).longValue();
        Maybe<Channel> channelMaybe = Maybe.just(CHANNEL1);

        when(paginatedEntries.getEntries()).thenReturn(entries);
        when(channelResolver.fromId(currentGuid)).thenReturn(channelMaybe);
        when(channelResolver.all()).thenReturn(channels);

        channelDataUpdater.addAliasesToChannel(paginatedEntries);

        verify(paginatedEntries).getEntries();
        verify(channelResolver).fromId(currentGuid);
        verify(channelResolver).all();

        assertEquals(expectedChannelWithAlias, CHANNEL1);

    }

    @Test
    public void testAddAvailableDateToChannel() {
        paginatedEntries = mock(PaginatedEntries.class);

        List<Entry> entries = Lists.newArrayList();
        Entry entry1 = entryForTesting();
        entries.add(entry1);

        List<Channel> channels = Lists.newArrayList();
        channels.add(CHANNEL1);

        Channel expectedChannelWithAvailableDate = new Channel(Publisher.METABROADCAST, "Channel 1", "a", true, MediaType.VIDEO, "http://channel1.com");

        //Turn this on to test when you turn it on in BtChannelDataUpdater.
        //expectedChannelWithAlias.addAdvertiseFrom(DateTime.now());

        long currentGuid = codec.decode(entry1.getGuid()).longValue();
        Maybe<Channel> channelMaybe = Maybe.just(CHANNEL1);

        when(paginatedEntries.getEntries()).thenReturn(entries);
        when(channelResolver.fromId(currentGuid)).thenReturn(channelMaybe);
        when(channelResolver.all()).thenReturn(channels);

        channelDataUpdater.addAvailableDateToChannel(paginatedEntries);

        verify(paginatedEntries).getEntries();
        verify(channelResolver).fromId(currentGuid);
        verify(channelResolver).all();

        assertEquals(expectedChannelWithAvailableDate, CHANNEL1);

    }

    public Entry entryForTesting() {
        Category category = new Category("S0312140", "subscription", "BT Sport Service on Vision");

        return new Entry("hk4g", 0, "Nick Toons",
                ImmutableList.of(category),
                ImmutableList.<Content>of(),
                true, null, null, false, true, DateTime.now(),
                LINEAR_CHANNEL_ID);

    }
}
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

import java.util.List;
import java.util.Set;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;

public class BtChannelDataUpdaterTest {
    private BtChannelDataUpdater channelDataUpdater;
    private PaginatedEntries paginatedEntries;
    private ChannelResolver channelResolver;
    private ChannelWriter channelWriter;
    private Maybe<Channel> channelMaybe;

    private static final String baseUri = "http://example.org/1/root";

    private final SubstitutionTableNumberCodec codec = new SubstitutionTableNumberCodec().lowerCaseOnly();

    private static final String ALIAS_NAMESPACE = "gb:bt:tv:mpx:vole:service";
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

        Channel testChannel = new Channel(Publisher.METABROADCAST, "Channel 1", "a", true, MediaType.VIDEO, "http://channel1.com");

        List<Channel> channels = Lists.newArrayList();

        channels.add(testChannel);
        Channel expectedChannelWithAlias = new Channel(Publisher.METABROADCAST, "Channel 1", "a", true, MediaType.VIDEO, "http://channel1.com");

        Alias alias = new Alias(ALIAS_NAMESPACE, LINEAR_CHANNEL_ID);
        expectedChannelWithAlias.addAlias(alias);

        long channelId = codec.decode(entry1.getGuid()).longValue();
        channelMaybe = Maybe.just(testChannel);

        when(paginatedEntries.getEntries()).thenReturn(entries);
        when(channelResolver.fromId(channelId)).thenReturn(channelMaybe);
        when(channelResolver.all()).thenReturn(channels);

        channelDataUpdater.addAliasesToChannel(paginatedEntries);

        verify(paginatedEntries).getEntries();
        verify(channelResolver).fromId(channelId);
        verify(channelWriter, times(2)).createOrUpdate(expectedChannelWithAlias);
        verify(channelResolver).all();

        Set<Alias> channelAliases = testChannel.getAliases();

        assertThat(channelAliases.contains(alias), is(true));
    }

    @Test
    public void testAddAvailableDateToChannel() {
        paginatedEntries = mock(PaginatedEntries.class);

        List<Entry> entries = Lists.newArrayList();
        Entry entry1 = entryForTesting();
        entries.add(entry1);

        Channel testChannel = new Channel(Publisher.METABROADCAST, "Channel 1", "a", true, MediaType.VIDEO, "http://channel1.com");

        List<Channel> channels = Lists.newArrayList();

        channels.add(testChannel);

        Channel expectedChannelWithAvailableDate = new Channel(Publisher.METABROADCAST, "Channel 1", "a", true, MediaType.VIDEO, "http://channel1.com");


        expectedChannelWithAvailableDate.setAdvertiseFrom(DateTime.now());

        long currentGuid = codec.decode(entry1.getGuid()).longValue();
        Maybe<Channel> channelMaybe = Maybe.just(testChannel);

        when(paginatedEntries.getEntries()).thenReturn(entries);
        when(channelResolver.fromId(currentGuid)).thenReturn(channelMaybe);
        when(channelResolver.all()).thenReturn(channels);

        channelDataUpdater.addAvailableDateToChannel(paginatedEntries);

        verify(paginatedEntries).getEntries();
        verify(channelResolver).fromId(currentGuid);
        verify(channelResolver).all();

        assertEquals(expectedChannelWithAvailableDate, testChannel);

    }

    public Entry entryForTesting() {
        Category category = new Category("S0312140", "subscription", "BT Sport Service on Vision");

        return new Entry("hk4g", 0, "Nick Toons",
                ImmutableList.of(category),
                ImmutableList.<Content>of(),
                true, null, null, false, true, 0,
                LINEAR_CHANNEL_ID);

    }
}
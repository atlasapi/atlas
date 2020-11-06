package org.atlasapi.remotesite.bt.channels;

import com.google.api.client.util.Lists;
import com.google.common.collect.ImmutableList;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelQuery;
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
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Set;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;

@RunWith(MockitoJUnitRunner.class)
public class BtChannelDataUpdaterTest {
    @Captor private ArgumentCaptor<Channel> channelBeingWrittenCaptor;
    @Mock private ChannelWriter channelWriter;

    private BtChannelDataUpdater channelDataUpdater;

    private final SubstitutionTableNumberCodec codec = SubstitutionTableNumberCodec.lowerCaseOnly();

    private static final String ALIAS_NAMESPACE = "gb:bt:tv:mpx:vole:service";
    private static final String LINEAR_CHANNEL_ID = "urn:BT:linear:service:751764";

    @Test
    public void testAliasesClearing() {
        PaginatedEntries paginatedEntries = mock(PaginatedEntries.class);
        ChannelResolver channelResolver = mock(ChannelResolver.class);
        when(channelResolver.fromUri(any(String.class))).thenReturn(Maybe.nothing());

        channelDataUpdater = BtChannelDataUpdater.builder()
                .withChannelResolver(channelResolver)
                .withChannelWriter(channelWriter)
                .withAliasNamespace(ALIAS_NAMESPACE)
                .withPublisher(Publisher.BT_TV_CHANNELS_TEST1)
                .build();

        List<Entry> entries = Lists.newArrayList();
        Entry entry2 = emptyEntryWithNoLinearChannelIdForTesting();
        entries.add(entry2);

        Channel testChannel2 = new Channel(Publisher.METABROADCAST, "Channel 2", "b", true, MediaType.VIDEO, "http://channel2.com");
        for (String mbid : entry2.getGuid()) {
            long channelId = codec.decode(mbid).longValue();
            testChannel2.setId(channelId);
            Alias shouldNotRemove1 = new Alias("gb:bt:tv:mpx:testing:service", "urn:BT:linear:service:700000");
            Alias shouldNotRemove2 = new Alias("bbcone", "urn:BT:linear:service:710000");
            Alias shouldRemove = new Alias("gb:bt:tv:mpx:vole:service", "urn:BT:linear:service:710000");
            testChannel2.addAlias(shouldNotRemove1);
            testChannel2.addAlias(shouldRemove);
            testChannel2.addAlias(shouldNotRemove2);

            Maybe<Channel> channelMaybe = Maybe.just(testChannel2);

            List<Channel> channels = Lists.newArrayList();

            channels.add(testChannel2);

            when(paginatedEntries.getEntries()).thenReturn(entries);
            when(channelResolver.fromId(channelId)).thenReturn(channelMaybe);
            when(channelWriter.createOrUpdate(channelBeingWrittenCaptor.capture())).thenReturn(testChannel2);
            when(channelResolver.allChannels(any(ChannelQuery.class))).thenReturn(channels);

            channelDataUpdater.addAliasesToChannel(paginatedEntries);

            verify(paginatedEntries).getEntries();
            verify(channelResolver).fromId(channelId);
            verify(channelResolver).allChannels(any(ChannelQuery.class));

            Set<Alias> channelAliases = testChannel2.getAliases();
            assertThat(channelAliases.contains(shouldNotRemove1), is(true));
            assertThat(channelAliases.contains(shouldNotRemove2), is(true));
            assertThat(channelAliases.contains(shouldRemove), is(false));

            Channel testChannel2BeingWrittenCaptured = channelBeingWrittenCaptor.getValue();

            Set<Alias> channelBeingWrittenAliases = testChannel2BeingWrittenCaptured.getAliases();

            assertThat(channelBeingWrittenAliases.contains(shouldNotRemove1), is(true));
            assertThat(channelBeingWrittenAliases.contains(shouldNotRemove2), is(true));
            assertThat(channelBeingWrittenAliases.contains(shouldRemove), is(false));
        }
    }

    @Test
    public void testAddAliasesToChannel() throws Exception {
        PaginatedEntries paginatedEntries = mock(PaginatedEntries.class);
        ChannelResolver channelResolver = mock(ChannelResolver.class);

        when(channelResolver.fromUri(any(String.class))).thenReturn(Maybe.nothing());

        channelDataUpdater = BtChannelDataUpdater.builder()
                .withChannelResolver(channelResolver)
                .withChannelWriter(channelWriter)
                .withAliasNamespace(ALIAS_NAMESPACE)
                .withPublisher(Publisher.BT_TV_CHANNELS_TEST1)
                .build();

        List<Entry> entries = Lists.newArrayList();
        Entry entry1 = entryForTesting();

        entries.add(entry1);
        List<String> guid = entry1.getGuid();

        Channel testChannel = new Channel(Publisher.METABROADCAST, "Channel 1",
                "a", true, MediaType.VIDEO, "http://channel1.com");
        for (String mbid : guid) {
            Channel environmentChannel = new Channel(Publisher.BT_TV_CHANNELS_TEST1, "Channel 1",
                    "a", true, MediaType.VIDEO, "http://dev1.tv-channels.bt.com/" + codec.decode(mbid));

            Alias shouldNotRemove = new Alias("bbcone", "urn:BT:linear:service:710000");
            testChannel.addAlias(shouldNotRemove);
            List<Channel> channels = Lists.newArrayList();

            channels.add(testChannel);
            channels.add(environmentChannel);

            Channel expectedChannelWithAlias = new Channel(Publisher.METABROADCAST, "Channel 1",
                    "a", true, MediaType.VIDEO, "http://channel1.com");

            Alias alias = new Alias(ALIAS_NAMESPACE, LINEAR_CHANNEL_ID);
            expectedChannelWithAlias.addAlias(alias);

            long channelId = codec.decode(mbid).longValue();
            testChannel.setId(channelId);
            Maybe<Channel> channelMaybe = Maybe.just(testChannel);

            when(paginatedEntries.getEntries()).thenReturn(entries);
            when(channelResolver.fromId(channelId)).thenReturn(channelMaybe);
            when(channelResolver.fromUri(any(String.class))).thenReturn(Maybe.just(environmentChannel));
            when(channelResolver.allChannels(any(ChannelQuery.class))).thenReturn(channels);

            channelDataUpdater.addAliasesToChannel(paginatedEntries);

            verify(paginatedEntries).getEntries();
            verify(channelResolver).fromId(channelId);
            verify(channelWriter).createOrUpdate(expectedChannelWithAlias);
            verify(channelResolver).allChannels(any(ChannelQuery.class));

            Set<Alias> channelAliases = testChannel.getAliases();
            Set<Alias> environmentAliases = environmentChannel.getAliases();

            assertFalse(channelAliases.contains(alias));
            assertTrue(environmentAliases.contains(alias));

            //Make sure that we don't remove the old aliases after adding the new one.
            assertThat(channelAliases.contains(shouldNotRemove), is(true));
        }
    }

    @Test
    public void testAddAvailableDateToChannel() {
        PaginatedEntries paginatedEntries = mock(PaginatedEntries.class);
        ChannelResolver channelResolver = mock(ChannelResolver.class);
        when(channelResolver.fromUri(any(String.class))).thenReturn(Maybe.nothing());

        channelDataUpdater = BtChannelDataUpdater.builder()
                .withChannelResolver(channelResolver)
                .withChannelWriter(channelWriter)
                .withAliasNamespace(ALIAS_NAMESPACE)
                .withPublisher(Publisher.BT_TV_CHANNELS_TEST1)
                .build();

        List<Entry> entries = Lists.newArrayList();
        Entry entry1 = entryForTesting();
        List<String> guid = entry1.getGuid();

        entries.add(entry1);

        for (String mbid : guid) {
            Channel testChannel = new Channel(Publisher.METABROADCAST, "Channel 1",
                    "a", true, MediaType.VIDEO, "http://channel1.com");
            Channel environmentChannel = new Channel(Publisher.BT_TV_CHANNELS_TEST1, "Channel 1",
                    "b", true, MediaType.VIDEO, "http://dev1.tv-channels.bt.com/" + mbid);

            List<Channel> channels = Lists.newArrayList();

            channels.add(testChannel);
            channels.add(environmentChannel);

            Channel expectedChannelWithAvailableDate = new Channel(Publisher.METABROADCAST, "Channel 1",
                    "a", true, MediaType.VIDEO, "http://channel1.com");

            expectedChannelWithAvailableDate.setAdvertiseFrom(DateTime.now());

            long channelId = codec.decode(mbid).longValue();
            testChannel.setId(channelId);

            Maybe<Channel> channelMaybe = Maybe.just(testChannel);

            when(paginatedEntries.getEntries()).thenReturn(entries);
            when(channelResolver.fromId(channelId)).thenReturn(channelMaybe);
            when(channelResolver.allChannels(any(ChannelQuery.class))).thenReturn(channels);
            when(channelResolver.fromUri(any(String.class))).thenReturn(Maybe.just(environmentChannel));

            channelDataUpdater.addAvailableDatesToChannel(paginatedEntries);

            verify(paginatedEntries).getEntries();
            verify(channelResolver).fromId(channelId);
            verify(channelResolver).allChannels(any(ChannelQuery.class));

            assertEquals(
                    new DateTime(1446556354000L),
                    environmentChannel.getAdvertiseFrom()
            );

            assertEquals(
                    new DateTime(1447556354000L),
                    environmentChannel.getAdvertiseTo()
            );
        }

    }

    public Entry entryForTesting() {
        Category category = new Category("S0312140", "subscription", "BT Sport Service on Vision");
        long availableDate = Long.valueOf("1446556354000");
        long availableToDate = Long.valueOf("1447556354000");

        return new Entry("hk4g", 0, "Nick Toons",
                ImmutableList.of(category),
                ImmutableList.<Content>of(),
                true, null, null, false, true, availableDate, availableToDate,
                LINEAR_CHANNEL_ID);

    }

    public Entry emptyEntryWithNoLinearChannelIdForTesting() {
        Category category = new Category("S0312140", "subscription", "BT Sport Service on Vision");

        return new Entry("hpdr", 0, "AMC From BT",
                ImmutableList.of(category),
                ImmutableList.<Content>of(),
                true, null, null, false, true, 0, 0,
                null);

    }
}
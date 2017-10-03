package org.atlasapi.remotesite.pa.channels;

import org.atlasapi.media.channel.TemporalField;
import org.atlasapi.remotesite.pa.channels.bindings.Channel;
import org.atlasapi.remotesite.pa.channels.bindings.Channels;
import org.atlasapi.remotesite.pa.channels.bindings.Name;
import org.atlasapi.remotesite.pa.channels.bindings.Names;
import org.atlasapi.remotesite.pa.channels.bindings.Station;

import com.metabroadcast.common.stream.MoreCollectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.junit.Before;
import org.junit.Test;

import static org.atlasapi.remotesite.pa.channels.PaChannelsIngester.CHANNEL_URI_PREFIX;
import static org.atlasapi.remotesite.pa.channels.PaChannelsIngester.STATION_URI_PREFIX;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.nullValue;
import static org.joda.time.DateTimeZone.UTC;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PaChannelsIngesterTest {

    private PaChannelsIngester ingester;

    @Before
    public void setUp() throws Exception {
        ingester = new PaChannelsIngester();
    }

    @Test
    public void processStationWithNoChannelsReturnsEmptyHierarchy() throws Exception {
        ChannelTree channelTree = ingester.processStation(
                getStation(
                        "1",
                        ImmutableList.of()
                ),
                ImmutableList.of()
        );

        assertThat(
                channelTree.getParent(),
                is(nullValue())
        );
        assertThat(
                channelTree.getChildren().isEmpty(),
                is(true)
        );
    }

    @Test
    public void processStationWithSingleChannelReturnsOneChannelWithNoParent() throws Exception {
        Channel channel = getChannel("2");

        ChannelTree channelTree = ingester.processStation(
                getStation(
                        "1",
                        ImmutableList.of(channel)
                ),
                ImmutableList.of()
        );

        assertThat(
                channelTree.getParent(),
                is(nullValue())
        );
        assertThat(
                channelTree.getChildren().size(),
                is(1)
        );
        assertThat(
                channelTree.getChildren().get(0).getUri(),
                is(CHANNEL_URI_PREFIX + channel.getId())
        );
    }

    @Test
    public void processStationWithMultipleChannelsReturnsStationAsParent() throws Exception {
        Channel firstChannel = getChannel("2");
        Channel secondChannel = getChannel("3");

        Station station = getStation(
                "1",
                ImmutableList.of(
                        firstChannel,
                        secondChannel
                )
        );

        ChannelTree channelTree = ingester.processStation(
                station,
                ImmutableList.of()
        );

        assertThat(
                channelTree.getParent().getUri(),
                is(STATION_URI_PREFIX + station.getId())
        );
        assertThat(
                channelTree.getChildren().size(),
                is(2)
        );
        assertThat(
                channelTree.getChildren()
                        .stream()
                        .map(org.atlasapi.media.channel.Channel::getUri)
                        .collect(MoreCollectors.toImmutableSet()),
                is(ImmutableSet.of(
                        CHANNEL_URI_PREFIX + firstChannel.getId(),
                        CHANNEL_URI_PREFIX + secondChannel.getId()
                ))
        );
    }

    @Test
    public void processUtvStationRenamesItToItv() throws Exception {
        // This is to enforce an end of life for the workaround described in MBST-18347 by failing
        // this test.
        DateTime endOfLifeDateTime = new DateTime(
                2017, 11, 1, 10, 0, UTC
        );

        if (DateTime.now().isAfter(endOfLifeDateTime)) {
            fail("Time to remove this workaround");
        }

        Station station = getStation(
                "7",
                ImmutableList.of(
                        getChannel("2"),
                        getChannel("3")
                )
        );

        ChannelTree channelTree = ingester.processStation(
                station,
                ImmutableList.of()
        );

        assertThat(
                channelTree.getParent().getUri(),
                is(STATION_URI_PREFIX + station.getId())
        );

        assertThat(
                channelTree.getParent().getTitle(),
                is("ITV")
        );

        Iterable<TemporalField<String>> titles = channelTree.getParent().getAllTitles();
        assertThat(
                Iterables.size(titles),
                is(1)
        );

        TemporalField<String> title = Iterables.get(titles, 0);

        assertThat(
                title.getValue(),
                is("ITV")
        );
        assertThat(
                title.getStartDate(),
                is(new LocalDate(1970, 1, 1))
        );
    }

    private Station getStation(String id, ImmutableList<Channel> channels) {
        Station station = new Station();

        Channels channelsGrouping = mock(Channels.class);
        when(channelsGrouping.getChannel()).thenReturn(channels);

        station.setId(id);
        station.setChannels(channelsGrouping);

        station.setNames(getNames("Station"));

        return station;
    }

    private Channel getChannel(String id) {
        Channel channel = new Channel();

        channel.setId(id);
        channel.setStartDate("2017-01-01");

        channel.setNames(getNames("Name"));

        return channel;
    }

    private Names getNames(String name) {
        Names names = mock(Names.class);
        Name nameObject = mock(Name.class);

        when(names.getName()).thenReturn(ImmutableList.of(nameObject));
        when(nameObject.getvalue()).thenReturn(name);
        when(nameObject.getStartDate()).thenReturn("1970-01-01");

        return names;
    }
}

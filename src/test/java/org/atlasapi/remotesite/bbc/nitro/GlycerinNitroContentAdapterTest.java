package org.atlasapi.remotesite.bbc.nitro;

import java.util.List;

import com.google.api.client.util.Lists;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.atlasapi.media.entity.Clip;
import org.atlasapi.media.entity.Item;
import org.atlasapi.persistence.content.people.QueuingPersonWriter;

import com.metabroadcast.atlas.glycerin.Glycerin;
import com.metabroadcast.atlas.glycerin.GlycerinException;
import com.metabroadcast.atlas.glycerin.GlycerinResponse;
import com.metabroadcast.atlas.glycerin.model.Availability;
import com.metabroadcast.atlas.glycerin.model.Broadcast;
import com.metabroadcast.atlas.glycerin.model.Episode;
import com.metabroadcast.atlas.glycerin.model.PidReference;
import com.metabroadcast.atlas.glycerin.model.Programme;
import com.metabroadcast.atlas.glycerin.model.Version;
import com.metabroadcast.atlas.glycerin.queries.AvailabilityQuery;
import com.metabroadcast.atlas.glycerin.queries.BroadcastsQuery;
import com.metabroadcast.atlas.glycerin.queries.ProgrammesQuery;
import com.metabroadcast.atlas.glycerin.queries.VersionsQuery;
import com.metabroadcast.common.time.Clock;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static com.metabroadcast.atlas.glycerin.queries.ProgrammesMixin.PEOPLE;
import static com.metabroadcast.atlas.glycerin.queries.ProgrammesMixin.TITLES;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class GlycerinNitroContentAdapterTest {

    @Mock Glycerin glycerin;
    @Mock QueuingPersonWriter personWriter;
    @Mock Clock clock;
    @Mock GlycerinNitroClipsAdapter clipsAdapter;

    @Mock GlycerinResponse<Programme> glycerinResponse;
    @Mock GlycerinResponse<Availability> availabilityResponse;
    @Mock GlycerinResponse<Broadcast> broadcastResponse;
    @Mock GlycerinResponse<Version> versionResponse;
    private final int pageSize = 30;

    private NitroContentAdapter contentAdapter;

    @Before
    public void setUp() {
        contentAdapter = new GlycerinNitroContentAdapter(
                glycerin, clipsAdapter, personWriter, clock, pageSize);
    }

    @Test
    public void fetchEpisodesGetsContent() throws NitroException, GlycerinException {
        String pid = "p42qpid";
        String title = "Episode 5";

        ProgrammesQuery query = ProgrammesQuery.builder()
                .withPid(pid)
                .withMixins(TITLES, PEOPLE)
                .withPageSize(pageSize)
                .build();

        Episode episode = new Episode();
        episode.setPid(pid);
        episode.setTitle(title);

        List<Episode> episodes = Lists.newArrayList();
        episodes.add(episode);

        when(clipsAdapter.clipsFor(Matchers.<Iterable<PidReference>>any()))
                .thenReturn(ImmutableListMultimap.<String, Clip>of());

        when(glycerinResponse.hasNext()).thenReturn(true);
        when(glycerinResponse.getResults()).thenReturn(
                ImmutableList.of(
                        Programme.valueOf(episode),
                        Programme.valueOf(episode)
                )
        );
        when(glycerinResponse.getNext()).thenReturn(glycerinResponse);

        when(availabilityResponse.hasNext()).thenReturn(false);
        when(availabilityResponse.getResults()).thenReturn(ImmutableList.<Availability>of());

        when(broadcastResponse.hasNext()).thenReturn(false);
        when(broadcastResponse.getResults()).thenReturn(ImmutableList.<Broadcast>of());

        when(versionResponse.hasNext()).thenReturn(false);
        when(versionResponse.getResults()).thenReturn(ImmutableList.<Version>of());

        when(glycerin.execute(query)).thenReturn(glycerinResponse);
        when(glycerin.execute(any(AvailabilityQuery.class))).thenReturn(availabilityResponse);
        when(glycerin.execute(any(BroadcastsQuery.class))).thenReturn(broadcastResponse);
        when(glycerin.execute(any(VersionsQuery.class))).thenReturn(versionResponse);

        Iterable<Item> items = contentAdapter.fetchEpisodes(query);
        Item item = Iterables.getFirst(items, null);
        assertEquals(title, item.getTitle());
    }
}
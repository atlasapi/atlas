package org.atlasapi.remotesite.bbc.nitro;

import static com.metabroadcast.atlas.glycerin.queries.ProgrammesMixin.PEOPLE;
import static com.metabroadcast.atlas.glycerin.queries.ProgrammesMixin.TITLES;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.when;

import org.atlasapi.media.entity.Clip;
import org.atlasapi.media.entity.Item;
import org.atlasapi.persistence.content.people.QueuingPersonWriter;
import org.atlasapi.remotesite.bbc.nitro.v1.NitroClient;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
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

@RunWith(MockitoJUnitRunner.class)
public class GlycerinNitroContentAdapterTest {

    @Mock NitroClient nitroClient;
    @Mock Glycerin glycerin;
    @Mock QueuingPersonWriter personWriter;
    @Mock Clock clock;
    @Mock GlycerinNitroClipsAdapter clipsAdapter;

    @Mock GlycerinResponse<Programme> glycerinResponse;
    @Mock GlycerinResponse<Availability> availabilityResponse;
    @Mock GlycerinResponse<Broadcast> broadcastResponse;
    @Mock GlycerinResponse<Version> versionResponse;
    private final int pageSize = 5;

    private NitroContentAdapter contentAdapter;

    @Before
    public void setUp() {
        contentAdapter = new GlycerinNitroContentAdapter(
                glycerin, nitroClient, clipsAdapter, personWriter, clock, pageSize, true);
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

        when(clipsAdapter.clipsFor(Matchers.<Iterable<PidReference>>any()))
                .thenReturn(ImmutableListMultimap.<String, Clip>of());

        when(glycerinResponse.hasNext()).thenReturn(false);
        when(glycerinResponse.getResults())
                .thenReturn(ImmutableList.of(Programme.valueOf(episode)));

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

        ImmutableSet<Item> items = contentAdapter.fetchEpisodes(query);
        Item item = Iterables.getOnlyElement(items);
        assertEquals(title, item.getTitle());
    }
}
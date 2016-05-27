package org.atlasapi.remotesite.bbc.nitro;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import org.atlasapi.media.entity.Item;
import org.atlasapi.persistence.content.people.QueuingPersonWriter;
import org.atlasapi.remotesite.bbc.nitro.extract.NitroEpisodeExtractor;

import com.metabroadcast.atlas.glycerin.Glycerin;
import com.metabroadcast.atlas.glycerin.GlycerinException;
import com.metabroadcast.atlas.glycerin.GlycerinResponse;
import com.metabroadcast.atlas.glycerin.model.Availability;
import com.metabroadcast.atlas.glycerin.model.AvailabilityOf;
import com.metabroadcast.atlas.glycerin.model.Broadcast;
import com.metabroadcast.atlas.glycerin.model.Episode;
import com.metabroadcast.atlas.glycerin.model.PidReference;
import com.metabroadcast.atlas.glycerin.model.Version;
import com.metabroadcast.atlas.glycerin.queries.AvailabilityQuery;
import com.metabroadcast.common.time.Clock;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

@Ignore
@RunWith(MockitoJUnitRunner.class)
public class PaginatedNitroItemSourcesTest {

    @Mock private ListeningExecutorService listeningExecutorService;
    @Mock private Glycerin glycerin;
    @Mock private GlycerinNitroClipsAdapter clipsAdapter;
    @Mock private GlycerinResponse<Availability> availabilityResponse;
    @Mock private GlycerinResponse<Broadcast> broadcastResponse;
    private NitroEpisodeExtractor nitroEpisodeExtractor;
    private PaginatedNitroItemSources paginatedNitroItemSources;
    private List<Episode> episodes;
    private final int pageSize = 30;
    private final String versionPid = "b07d2xrxb";
    @Mock private Clock clock;
    @Mock private QueuingPersonWriter personWriter;
    @Mock private ListenableFuture listenableFuture;
    @Mock private Availability availability;
    @Mock private AvailabilityOf availabilityOf;
    @Mock private Broadcast broadcast;
    @Mock private Version version;

    @Before
    public void setUp() throws Exception {
        this.nitroEpisodeExtractor = new NitroEpisodeExtractor(clock, personWriter);
        this.episodes = createEpisodes();

        this.paginatedNitroItemSources = new PaginatedNitroItemSources(
                ImmutableList.of(episodes),
                listeningExecutorService,
                glycerin,
                pageSize,
                nitroEpisodeExtractor,
                clipsAdapter,
                ImmutableListMultimap.<String, Broadcast>of()
        );
    }

    private ImmutableList<Episode> createEpisodes() {
        ImmutableList.Builder<Episode> episodes = ImmutableList.builder();

        Episode.Version version = new Episode.Version();
        version.setPid(versionPid);

        Episode episode1 = new Episode();
        episode1.setPid(versionPid);
        episode1.setUri("testUri1");
        episode1.setVersion(version);
        episodes.add(episode1);

        Episode episode2 = new Episode();
        episode2.setPid("b07d2xrs");
        episode2.setUri("testUri2");
        episodes.add(episode2);

        Episode episode3 = new Episode();
        episode3.setPid("b07d2xrba");
        episode3.setUri("testUri3");
        episodes.add(episode3);


        return episodes.build();
    }

    @Test
    public void itemHasCorrectVersions()
            throws GlycerinException, ExecutionException, InterruptedException {
        Iterator<List<Item>> episodesIterator = paginatedNitroItemSources.iterator();

        // Setting availabilities
        when(glycerin.execute(any(AvailabilityQuery.class))).thenReturn(availabilityResponse);

        when(listeningExecutorService.submit(any(Callable.class))).thenReturn(
                Futures.immediateFuture(
                        ImmutableList.of(availability)
                ),
                Futures.immediateFuture(
                        ImmutableList.of(broadcast)
                ),
                Futures.immediateFuture(
                        ImmutableList.of(version)
                )
        );

        when(availability.getAvailabilityOf()).thenReturn(ImmutableList.of(availabilityOf));
        when(availabilityOf.getResultType()).thenReturn("episode", "version");
        when(availabilityOf.getPid()).thenReturn("b07d2xrg");

        PidReference correctPidRef = new PidReference();
        correctPidRef.setPid(versionPid);
        correctPidRef.setResultType("episode");

        PidReference incorrectPidRef = new PidReference();
        incorrectPidRef.setPid("b07d2xrxf");
        incorrectPidRef.setResultType("episode");

        when(broadcast.getBroadcastOf()).thenReturn(ImmutableList.of(incorrectPidRef));

        when(version.getVersionOf()).thenReturn(correctPidRef);
        when(version.getPid()).thenReturn(versionPid);

        List<Item> episodes = episodesIterator.next();
        assertTrue(episodes.size() == 3);
        assertTrue(episodes.get(0).getVersions().iterator().next().getCanonicalUri()
                .equals(String.format("http://nitro.bbc.co.uk/programmes/%s", versionPid))
        );
    }
}
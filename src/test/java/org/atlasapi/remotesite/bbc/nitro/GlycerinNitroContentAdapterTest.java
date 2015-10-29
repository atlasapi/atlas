package org.atlasapi.remotesite.bbc.nitro;

import static com.metabroadcast.atlas.glycerin.queries.ProgrammesMixin.PEOPLE;
import static com.metabroadcast.atlas.glycerin.queries.ProgrammesMixin.TITLES;
import static org.junit.Assert.assertEquals;

import org.atlasapi.media.entity.Item;
import org.atlasapi.persistence.content.people.QueuingPersonWriter;
import org.atlasapi.remotesite.bbc.nitro.v1.NitroClient;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.ImmutableSet;
import com.metabroadcast.atlas.glycerin.Glycerin;
import com.metabroadcast.atlas.glycerin.queries.ProgrammesQuery;
import com.metabroadcast.common.time.Clock;

@RunWith(MockitoJUnitRunner.class)
public class GlycerinNitroContentAdapterTest {

    @Mock NitroClient nitroClient;
    @Mock Glycerin glycerin;
    @Mock QueuingPersonWriter personWriter;
    @Mock Clock clock;
    private final int pageSize = 5;

    private NitroContentAdapter contentAdapter;

    @Before
    public void setUp() {
        contentAdapter = new GlycerinNitroContentAdapter(
                glycerin, nitroClient, personWriter, clock, pageSize);
    }

    @Test
    public void fetchEpisodes() throws NitroException {
        String pid = "Q pid";
        ImmutableSet<Item> items = contentAdapter
                .fetchEpisodes(ProgrammesQuery.builder()
                        .withPid(pid)
                        .withMixins(TITLES, PEOPLE)
                        .withPageSize(pageSize)
                        .build());
        assertEquals(1, items.size());
    }
}
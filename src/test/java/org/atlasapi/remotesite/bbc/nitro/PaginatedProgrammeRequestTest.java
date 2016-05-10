package org.atlasapi.remotesite.bbc.nitro;

import com.google.common.collect.ImmutableList;
import com.metabroadcast.atlas.glycerin.Glycerin;
import com.metabroadcast.atlas.glycerin.GlycerinException;
import com.metabroadcast.atlas.glycerin.GlycerinResponse;
import com.metabroadcast.atlas.glycerin.model.Programme;
import com.metabroadcast.atlas.glycerin.queries.ProgrammesQuery;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Iterator;

import static com.metabroadcast.atlas.glycerin.queries.ProgrammesMixin.PEOPLE;
import static com.metabroadcast.atlas.glycerin.queries.ProgrammesMixin.TITLES;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class PaginatedProgrammeRequestTest {

    @Mock private Glycerin client;
    @Mock private GlycerinResponse<Programme> glycerinResponse;
    @Mock private Programme programme;
    private ProgrammesQuery programmesQuery;

    private PaginatedProgrammeRequest paginatedProgrammeRequest;

    @Before
    public void setUp() throws Exception {
        this.programmesQuery = ProgrammesQuery.builder().withQ("test").build();

        String pid = "p42qpid";
        int pageSize = 5;
        this.programmesQuery = ProgrammesQuery.builder()
                .withPid(pid)
                .withMixins(TITLES, PEOPLE)
                .withPageSize(pageSize)
                .build();

        this.paginatedProgrammeRequest = new PaginatedProgrammeRequest(client, ImmutableList.of(programmesQuery));
    }

    @Test
    public void ifNextProgrammeIsSelectedWhenCurrentIsEmptyReturnsNextProgrammeIterable() throws GlycerinException {
        ImmutableList.Builder<Programme> programmesList = ImmutableList.builder();
        this.paginatedProgrammeRequest = new PaginatedProgrammeRequest(client, ImmutableList.of(programmesQuery, programmesQuery));

        when(client.execute(any(ProgrammesQuery.class))).thenReturn(glycerinResponse, glycerinResponse);
        when(glycerinResponse.getNext()).thenReturn(glycerinResponse);
        when(glycerinResponse.getResults()).thenReturn(programmesList.build());
        when(glycerinResponse.getNext()).thenReturn(glycerinResponse);
        when(glycerinResponse.getResults()).thenReturn(ImmutableList.of(programme));

        Iterator<Programme> programmes = paginatedProgrammeRequest.iterator();
        assertTrue(programmes.hasNext());
    }

    @Test
    public void whenCurrentProgrammeIsEmptyAndNextProgrammeIsEmptyReturnsEmptyIterable() throws GlycerinException {
        ImmutableList.Builder<Programme> programmesList = ImmutableList.builder();
        this.paginatedProgrammeRequest = new PaginatedProgrammeRequest(client, ImmutableList.of(programmesQuery, programmesQuery));

        when(client.execute(any(ProgrammesQuery.class))).thenReturn(glycerinResponse, glycerinResponse);
        when(glycerinResponse.getNext()).thenReturn(glycerinResponse);
        when(glycerinResponse.getResults()).thenReturn(programmesList.build());
        when(glycerinResponse.getNext()).thenReturn(glycerinResponse);
        when(glycerinResponse.getResults()).thenReturn(programmesList.build());

        Iterator<Programme> programmes = paginatedProgrammeRequest.iterator();
        assertFalse(programmes.hasNext());
    }

    @Test
    public void ifThereIsNoMoreProgrammesReturnEmptyIterable() throws GlycerinException {
        ImmutableList.Builder<ProgrammesQuery> programmesList = ImmutableList.builder();
        this.paginatedProgrammeRequest = new PaginatedProgrammeRequest(client, programmesList.build());

        Iterator<Programme> programmes = paginatedProgrammeRequest.iterator();
        assertFalse(programmes.hasNext());
    }

    @Test
    public void gettingNextProgrammeReturnsNextProgrammeIterable() throws GlycerinException {
        ImmutableList.Builder<Programme> programmesList = ImmutableList.builder();
        this.paginatedProgrammeRequest = new PaginatedProgrammeRequest(client, ImmutableList.of(programmesQuery, programmesQuery));

        when(client.execute(any(ProgrammesQuery.class))).thenReturn(glycerinResponse, glycerinResponse);
        when(glycerinResponse.getNext()).thenReturn(glycerinResponse);
        when(glycerinResponse.getResults()).thenReturn(programmesList.build());
        when(glycerinResponse.getNext()).thenReturn(glycerinResponse);
        when(glycerinResponse.getResults()).thenReturn(ImmutableList.of(programme));

        Iterator<Programme> programmes = paginatedProgrammeRequest.iterator();
        programmes.hasNext();
        programmes.next();
        assertTrue(programmes.hasNext());
    }

    @Test
    public void gettingNextPageInProgrammeReturnsNextPageIterable() throws GlycerinException {
        this.paginatedProgrammeRequest = new PaginatedProgrammeRequest(client, ImmutableList.of(programmesQuery, programmesQuery));

        when(client.execute(any(ProgrammesQuery.class))).thenReturn(glycerinResponse);
        when(glycerinResponse.getNext()).thenReturn(glycerinResponse);
        when(glycerinResponse.getResults()).thenReturn(ImmutableList.of(programme));
        when(glycerinResponse.getNext()).thenReturn(glycerinResponse);
        when(glycerinResponse.hasNext()).thenReturn(true);

        Iterator<Programme> programmes = paginatedProgrammeRequest.iterator();
        programmes.hasNext();
        programmes.next();
        assertTrue(programmes.hasNext());
    }

    @Test
    public void gettingNextElementFromProgrammeReturnsNextElementIterable() throws GlycerinException {
        this.paginatedProgrammeRequest = new PaginatedProgrammeRequest(client, ImmutableList.of(programmesQuery, programmesQuery));

        when(client.execute(any(ProgrammesQuery.class))).thenReturn(glycerinResponse);
        when(glycerinResponse.getResults()).thenReturn(ImmutableList.of(programme));
        when(glycerinResponse.hasNext()).thenReturn(false);
        when(glycerinResponse.getNext()).thenReturn(glycerinResponse);
        when(glycerinResponse.hasNext()).thenReturn(true);

        Iterator<Programme> programmes = paginatedProgrammeRequest.iterator();
        programmes.hasNext();
        assertTrue(programmes.hasNext());
    }
}
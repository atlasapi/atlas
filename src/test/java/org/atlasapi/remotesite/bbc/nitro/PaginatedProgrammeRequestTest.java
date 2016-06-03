package org.atlasapi.remotesite.bbc.nitro;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import com.metabroadcast.atlas.glycerin.Glycerin;
import com.metabroadcast.atlas.glycerin.GlycerinException;
import com.metabroadcast.atlas.glycerin.GlycerinResponse;
import com.metabroadcast.atlas.glycerin.model.Programme;
import com.metabroadcast.atlas.glycerin.queries.ProgrammesQuery;

import com.google.common.collect.ImmutableList;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static com.metabroadcast.atlas.glycerin.queries.ProgrammesMixin.PEOPLE;
import static com.metabroadcast.atlas.glycerin.queries.ProgrammesMixin.TITLES;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class PaginatedProgrammeRequestTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Mock private Glycerin client;
    @Mock private GlycerinResponse<Programme> glycerinResponse;
    @Mock private Programme programme;

    private final ProgrammesQuery programmesQuery = ProgrammesQuery.builder()
            .withPid("p42qpid")
            .withMixins(TITLES, PEOPLE)
            .withPageSize(5)
            .build();

    @Test
    public void ifNextProgrammeIsSelectedWhenCurrentIsEmptyReturnsNextProgrammeIterable() throws GlycerinException {
        ImmutableList.Builder<Programme> programmesList = ImmutableList.builder();
        PaginatedProgrammeRequest paginatedProgrammeRequest = new PaginatedProgrammeRequest(
                client,
                ImmutableList.of(programmesQuery, programmesQuery)
        );

        when(client.execute(any(ProgrammesQuery.class))).thenReturn(glycerinResponse);
        when(glycerinResponse.getNext()).thenReturn(glycerinResponse);
        when(glycerinResponse.getResults()).thenReturn(programmesList.build());
        when(glycerinResponse.getNext()).thenReturn(glycerinResponse);
        when(glycerinResponse.getResults()).thenReturn(ImmutableList.of(programme));
        when(client.execute(any(ProgrammesQuery.class))).thenReturn(glycerinResponse);
        when(glycerinResponse.hasNext()).thenReturn(true);

        Iterator<List<Programme>> programmes = paginatedProgrammeRequest.iterator();
        programmes.hasNext();
        programmes.next();
        assertTrue(programmes.hasNext());
    }

    @Test
    public void whenCurrentProgrammeIsEmptyAndNextProgrammeIsEmptyReturnsEmptyIterable() throws GlycerinException {
        ImmutableList.Builder<Programme> programmesList = ImmutableList.builder();
        PaginatedProgrammeRequest paginatedProgrammeRequest = new PaginatedProgrammeRequest(
                client,
                ImmutableList.<ProgrammesQuery>of()
        );

        when(client.execute(any(ProgrammesQuery.class))).thenReturn(glycerinResponse, glycerinResponse);
        when(glycerinResponse.getNext()).thenReturn(glycerinResponse);
        when(glycerinResponse.getResults()).thenReturn(programmesList.build());
        when(glycerinResponse.getNext()).thenReturn(glycerinResponse);
        when(glycerinResponse.getResults()).thenReturn(programmesList.build());

        Iterator<List<Programme>> programmes = paginatedProgrammeRequest.iterator();
        assertFalse(programmes.hasNext());
    }

    @Test
    public void ifThereIsNoMoreProgrammesReturnEmptyIterable() throws GlycerinException {
        ImmutableList.Builder<ProgrammesQuery> programmesList = ImmutableList.builder();
        PaginatedProgrammeRequest paginatedProgrammeRequest = new PaginatedProgrammeRequest(
                client,
                programmesList.build()
        );

        when(client.execute(any(ProgrammesQuery.class))).thenReturn(glycerinResponse);
        when(glycerinResponse.hasNext()).thenReturn(false);

        Iterator<List<Programme>> programmes = paginatedProgrammeRequest.iterator();
        assertFalse(programmes.hasNext());
    }

    @Test
    public void gettingNextProgrammeReturnsNextProgrammeIterable() throws GlycerinException {
        ImmutableList.Builder<Programme> programmesList = ImmutableList.builder();

        ImmutableList.Builder<ProgrammesQuery> programmesQueries = ImmutableList.builder();
        for (int i = 0; i < 35; i++) {
            programmesQueries.add(programmesQuery);
        }

        PaginatedProgrammeRequest paginatedProgrammeRequest = new PaginatedProgrammeRequest(
                client,
                programmesQueries.build()
        );

        when(client.execute(any(ProgrammesQuery.class))).thenReturn(glycerinResponse);
        when(glycerinResponse.getNext()).thenReturn(glycerinResponse);
        when(glycerinResponse.getResults()).thenReturn(programmesList.build());
        when(glycerinResponse.getNext()).thenReturn(glycerinResponse);
        when(glycerinResponse.getResults()).thenReturn(ImmutableList.of(programme));
        when(glycerinResponse.hasNext()).thenReturn(true);

        Iterator<List<Programme>> programmes = paginatedProgrammeRequest.iterator();
        programmes.hasNext();
        programmes.next();
        assertTrue(programmes.hasNext());
    }

    @Test
    public void gettingNextPageInProgrammeReturnsNextPageIterable() throws GlycerinException {
        PaginatedProgrammeRequest paginatedProgrammeRequest = new PaginatedProgrammeRequest(
                client,
                ImmutableList.of(programmesQuery, programmesQuery)
        );

        when(client.execute(any(ProgrammesQuery.class))).thenReturn(glycerinResponse);
        when(glycerinResponse.getNext()).thenReturn(glycerinResponse);
        when(glycerinResponse.getResults()).thenReturn(ImmutableList.of(programme));
        when(glycerinResponse.getNext()).thenReturn(glycerinResponse);
        when(glycerinResponse.hasNext()).thenReturn(true);

        Iterator<List<Programme>> programmes = paginatedProgrammeRequest.iterator();
        programmes.hasNext();
        programmes.next();
        assertTrue(programmes.hasNext());
    }

    @Test
    public void gettingNextElementFromProgrammeReturnsNextElementIterable() throws GlycerinException {
        PaginatedProgrammeRequest paginatedProgrammeRequest = new PaginatedProgrammeRequest(
                client,
                ImmutableList.of(programmesQuery, programmesQuery)
        );

        when(client.execute(any(ProgrammesQuery.class))).thenReturn(glycerinResponse);
        when(glycerinResponse.getResults()).thenReturn(ImmutableList.of(programme));
        when(glycerinResponse.hasNext()).thenReturn(false);
        when(glycerinResponse.getNext()).thenReturn(glycerinResponse);
        when(glycerinResponse.hasNext()).thenReturn(true);

        Iterator<List<Programme>> programmes = paginatedProgrammeRequest.iterator();
        assertTrue(programmes.hasNext());
    }

    @Test
    public void throwsOnNextWhenOutOfElements() throws GlycerinException {
        PaginatedProgrammeRequest paginatedProgrammeRequest = new PaginatedProgrammeRequest(
                client,
                ImmutableList.<ProgrammesQuery>of()
        );

        thrown.expect(NoSuchElementException.class);
        paginatedProgrammeRequest.iterator().next();
    }
}
package org.atlasapi.equiv.handlers;

import java.util.List;
import java.util.Optional;

import org.atlasapi.equiv.results.EquivalenceResult;
import org.atlasapi.equiv.results.description.DefaultDescription;
import org.atlasapi.equiv.results.description.ReadableDescription;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidate;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.equiv.scorers.TitleMatchingItemScorer;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;

import com.metabroadcast.columbus.telescope.api.Event;
import com.metabroadcast.columbus.telescope.api.Ingester;
import com.metabroadcast.columbus.telescope.api.Task;
import com.metabroadcast.columbus.telescope.client.IngestTelescopeClientImpl;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ColumbusTelescopeReportHandlerTest {

    @Mock private IngestTelescopeClientImpl telescopeClient;
    @Mock private Task task;
    @Captor private ArgumentCaptor<Iterable<Event>> eventCaptor;
    private final TitleMatchingItemScorer scorer = new TitleMatchingItemScorer();

    private ReadableDescription readableDescription = mock(ReadableDescription.class);

    private ColumbusTelescopeReportHandler telescopeReportHandler;

    @Before
    public void setUp() throws Exception {
        this.telescopeReportHandler = new ColumbusTelescopeReportHandler();
        this.readableDescription = new DefaultDescription();
        readableDescription.appendText("Publisher: pressassociation.com");
        readableDescription.appendText("Percent Above Next Best Match Extractor");
        readableDescription.appendText("Episode parent filter");
        readableDescription.appendText("Item has no Container");
    }

    @Test
    public void parsingOfEquivResultForRawElement() {
        Optional<String> taskId = Optional.of("dfafa");

        when(telescopeClient.startIngest(any(Ingester.class))).thenReturn(task);
        when(task.getId()).thenReturn(taskId);

        EquivalenceResult<Item> result = createEquivResult();

        telescopeReportHandler.handle(result, taskId, telescopeClient);

        verify(telescopeClient).createEvents(eventCaptor.capture());

        Event event = eventCaptor.getValue().iterator().next();
        assertThat(event.getTaskId().get(), is("dfafa"));
        assertThat(event.getType(), is(Event.Type.EQUIVALENCE));

        assertTrue(event.getEntityState().get().getRaw().isPresent());
        assertThat(event.getEntityState().get().getRaw().get(), is("{\"parts\":[\"Publisher: pressassociation.com\"," +
                "\"Percent Above Next Best Match Extractor\",\"Episode parent filter\",\"Item has no Container\"]," +
                "\"currentPart\":[\"Publisher: pressassociation.com\",\"Percent Above Next Best Match Extractor\"," +
                "\"Episode parent filter\",\"Item has no Container\"],\"parentParts\":[]}"));
        assertThat(event.getEntityState().get().getRawMime().get(), is("application/json"));
    }

    private EquivalenceResult<Item> createEquivResult() {
        Item item = new Item("main", "sdaf88", Publisher.BBC);
        item.setId(23912391L);
        Item equivItem = new Item("equiv", "dsaf4", Publisher.INTERNET_VIDEO_ARCHIVE);
        equivItem.setId(12631234L);

        List<ScoredCandidates> scores = Lists.newArrayList();
        ScoredCandidates candidate = DefaultScoredCandidates.fromSource("equiv")
                .addEquivalent(equivItem, Score.ONE)
                .build();
        scores.add(candidate);

        Multimap<Publisher, ScoredCandidate<Item>> strongCandidates = ArrayListMultimap.create();
        strongCandidates.put(Publisher.INTERNET_VIDEO_ARCHIVE, ScoredCandidate.valueOf(item, Score.ONE));

        DefaultDescription description = new DefaultDescription();
        description.startStage("A good description has been written right here and now!");

        return new EquivalenceResult(
                item,
                scores,
                null,
                strongCandidates,
                readableDescription
        );
    }
}
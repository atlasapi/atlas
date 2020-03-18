package org.atlasapi.equiv.handlers;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import junit.framework.TestCase;
import org.atlasapi.equiv.ContentRef;
import org.atlasapi.equiv.results.EquivalenceResult;
import org.atlasapi.equiv.results.EquivalenceResults;
import org.atlasapi.equiv.results.description.DefaultDescription;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidate;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.media.entity.Described;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.lookup.LookupWriter;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Set;

import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyCollectionOf;
import static org.mockito.Matchers.anySetOf;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class LookupWritingEquivalenceHandlerTest extends TestCase {

    private static final Function<Item, ScoredCandidate<Item>> RANDOM_SCORE = input -> {
        //Chosen by fair dice roll / 10. Guaranteed to be random.
        return ScoredCandidate.valueOf(input, Score.valueOf(0.4));
    };

    private final LookupWriter lookupWriter = mock(LookupWriter.class);
    private final Set<Publisher> publishers = ImmutableSet.of(
            Publisher.BBC,
            Publisher.PA,
            Publisher.ITV
    );

    private final LookupWritingEquivalenceHandler<Item> updater =
            new LookupWritingEquivalenceHandler<>(
                    lookupWriter,
                    publishers
            );

    private final Item content = new Item("item", "c:item", Publisher.BBC);
    private final Item equiv1 = new Item("equiv1", "c:equiv1", Publisher.PA);
    private final Item equiv2 = new Item("equiv2", "c:equiv2", Publisher.ITV);

    private final ContentRef contentRef = ContentRef.valueOf(content);
    private final ContentRef equiv1Ref = ContentRef.valueOf(equiv1);
    private final ContentRef equiv2Ref = ContentRef.valueOf(equiv2);

    @Test
    public void testWritesLookups() {
        when(lookupWriter.writeLookup(
                Matchers.any(ContentRef.class),
                anyCollectionOf(ContentRef.class),
                anySetOf(Publisher.class)
        ))
                .thenReturn(Optional.of(ImmutableSet.of()));

        updater.handle(equivResultsFor(content, ImmutableList.of(equiv1, equiv2)));

        verify(lookupWriter).writeLookup(
                argThat(is(contentRef)),
                argThat(hasItems(equiv1Ref, equiv2Ref)),
                argThat(is(publishers))
        );
    }

    @Test
    public void testReturnsTrueIfLookupWasWritten() {
        when(lookupWriter.writeLookup(
                Matchers.any(ContentRef.class),
                anyCollectionOf(ContentRef.class),
                anySetOf(Publisher.class)
        ))
                .thenReturn(Optional.of(ImmutableSet.of()));

        boolean handled = updater.handle(equivResultsFor(content, ImmutableList.of(equiv1, equiv2)));

        assertThat(handled, is(true));
    }

    @Test
    public void testReturnsFalseIfLookupWasNop() {
        when(lookupWriter.writeLookup(
                Matchers.any(ContentRef.class),
                anyCollectionOf(ContentRef.class),
                anySetOf(Publisher.class)
        ))
                .thenReturn(Optional.absent());

        boolean handled = updater.handle(equivResultsFor(content, ImmutableList.of(equiv1, equiv2)));

        assertThat(handled, is(false));
    }

    @Test
    public void
    testWritesAllLookupsFromMultipleEquivalenceResults() {
        when(lookupWriter.writeLookup(
                Matchers.any(ContentRef.class),
                anyCollectionOf(ContentRef.class),
                anySetOf(Publisher.class)
        ))
                .thenReturn(Optional.of(ImmutableSet.of()));

        EquivalenceResult<Item> equivResult1 = equivResultFor(
                content,
                ImmutableList.of(equiv1)
        );

        EquivalenceResult<Item> equivResult2 = equivResultFor(
                content,
                ImmutableList.<Item>of(equiv2)
        );

        EquivalenceResults<Item> equivResults = new EquivalenceResults<>(
                content,
                ImmutableList.of(equivResult1, equivResult2),
                new DefaultDescription()
        );

        updater.handle(equivResults);

        verify(lookupWriter).writeLookup(
                argThat(is(contentRef)),
                argThat(hasItems(equiv1Ref, equiv2Ref)),
                argThat(is(publishers))
        );
    }

    private EquivalenceResult<Item> equivResultFor(Item content, Iterable<Item> equivalents) {
        Multimap<Publisher, ScoredCandidate<Item>> strong = Multimaps.transformValues(Multimaps
                .index(
                        equivalents,
                        Described::getPublisher
                ), RANDOM_SCORE);
        return new EquivalenceResult<>(
                content,
                ImmutableList.<ScoredCandidates<Item>>of(),
                null,
                strong,
                null
        );
    }

    private EquivalenceResults<Item> equivResultsFor(Item content, Iterable<Item> equivalents) {
        return new EquivalenceResults<>(
                content,
                ImmutableList.of(equivResultFor(content, equivalents)),
                null
        );
    }
}

package org.atlasapi.equiv.handlers;

import java.util.Set;

import org.atlasapi.equiv.ContentRef;
import org.atlasapi.equiv.results.EquivalenceResult;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidate;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.media.entity.Described;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.lookup.LookupWriter;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import junit.framework.TestCase;
import org.joda.time.Duration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.runners.MockitoJUnitRunner;

import static org.hamcrest.Matchers.any;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyCollectionOf;
import static org.mockito.Matchers.anySetOf;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
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
    private final Duration cacheDuration = new Duration(5);

    private final LookupWritingEquivalenceHandler<Item> updater =
            new LookupWritingEquivalenceHandler<>(
                    lookupWriter,
                    publishers,
                    cacheDuration
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

        updater.handle(equivResultFor(content, ImmutableList.of(equiv1, equiv2)));

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

        boolean handled = updater.handle(equivResultFor(content, ImmutableList.of(equiv1, equiv2)));

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

        boolean handled = updater.handle(equivResultFor(content, ImmutableList.of(equiv1, equiv2)));

        assertThat(handled, is(false));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void
    testDoesntWriteLookupsForItemWhichWasSeenAsEquivalentButDoesntAssertAnyEquivalences() {
        when(lookupWriter.writeLookup(
                Matchers.any(ContentRef.class),
                anyCollectionOf(ContentRef.class),
                anySetOf(Publisher.class)
        ))
                .thenReturn(Optional.of(ImmutableSet.of()));

        EquivalenceResult<Item> equivResult = equivResultFor(
                content,
                ImmutableList.of(equiv1, equiv2)
        );
        EquivalenceResult<Item> noEquivalences = equivResultFor(
                equiv1,
                ImmutableList.<Item>of()
        );

        assertThat(
                updater.handle(equivResult),
                is(true)
        );

        assertThat(
                updater.handle(noEquivalences),
                is(false)
        );

        verify(lookupWriter).writeLookup(
                argThat(is(contentRef)),
                argThat(hasItems(equiv1Ref, equiv2Ref)),
                argThat(is(publishers))
        );
        verify(lookupWriter, never()).writeLookup(
                argThat(is(equiv1Ref)),
                argThat(any(Iterable.class)),
                argThat(is(publishers))
        );
    }

    @Test
    public void
    testWritesLookupsForItemWhichWasSeenAsEquivalentButDoesntAssertAnyEquivalencesWhenCacheTimesOut()
            throws Exception {
        when(lookupWriter.writeLookup(
                Matchers.any(ContentRef.class),
                anyCollectionOf(ContentRef.class),
                anySetOf(Publisher.class)
        ))
                .thenReturn(Optional.of(ImmutableSet.of()));

        EquivalenceResult<Item> equivResult1 = equivResultFor(
                content,
                ImmutableList.of(equiv1, equiv2)
        );

        EquivalenceResult<Item> equivResult2 = equivResultFor(
                equiv1,
                ImmutableList.<Item>of(content)
        );

        updater.handle(equivResult1);
        Thread.sleep(cacheDuration.getMillis() * 2);
        updater.handle(equivResult2);

        verify(lookupWriter).writeLookup(
                argThat(is(contentRef)),
                argThat(hasItems(equiv1Ref, equiv2Ref)),
                argThat(is(publishers))
        );
        verify(lookupWriter).writeLookup(
                argThat(is(equiv1Ref)),
                argThat(hasItems(contentRef)),
                argThat(is(publishers))
        );

    }

    private EquivalenceResult<Item> equivResultFor(Item content, Iterable<Item> equivalents) {
        Multimap<Publisher, ScoredCandidate<Item>> strong = Multimaps.transformValues(Multimaps
                .index(
                equivalents,
                        Described::getPublisher
        ), RANDOM_SCORE);
        return new EquivalenceResult<Item>(
                content,
                ImmutableList.<ScoredCandidates<Item>>of(),
                null,
                strong,
                null
        );
    }
}

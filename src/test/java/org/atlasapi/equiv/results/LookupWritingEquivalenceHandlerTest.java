package org.atlasapi.equiv.results;

import static org.hamcrest.Matchers.any;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.util.Map;
import java.util.Set;

import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import junit.framework.TestCase;

import org.atlasapi.equiv.ContentRef;
import org.atlasapi.equiv.handlers.LookupWritingEquivalenceHandler;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidate;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.lookup.LookupWriter;
import org.joda.time.Duration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

@RunWith(MockitoJUnitRunner.class)
public class LookupWritingEquivalenceHandlerTest extends TestCase {
    
    private final LookupWriter lookupWriter = mock(LookupWriter.class);
    private final Set<Publisher> publishers = ImmutableSet.of(Publisher.BBC,Publisher.PA, Publisher.ITV);
    private final LookupWritingEquivalenceHandler<Item> updater = new LookupWritingEquivalenceHandler<Item>(lookupWriter, publishers);

    private final Item content = new Item("item","c:item", Publisher.BBC);
    private final Item equiv1 = new Item("equiv1","c:equiv1",Publisher.PA);
    private final Item equiv2 = new Item("equiv2","c:equiv2",Publisher.ITV);
    
    private final ContentRef contentRef = ContentRef.valueOf(content);
    private final ContentRef equiv1Ref = ContentRef.valueOf(equiv1);
    private final ContentRef equiv2Ref = ContentRef.valueOf(equiv2);
    
    @Test
    public void testWritesLookups() {
        
        updater.handle(equivResultFor(content, ImmutableList.of(equiv1,equiv2)), null);
        
        verify(lookupWriter).writeLookup(argThat(is(contentRef)), argThat(hasItems(equiv1Ref,equiv2Ref)), argThat(is(publishers)));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testDoesntWriteLookupsForItemWhichWasSeenAsEquivalentButDoesntAssertAnyEquivalences() {
        
        final EquivalenceResult<Item> equivResult = equivResultFor(content, ImmutableList.of(equiv1,equiv2));
        final EquivalenceResult<Item> noEquivalences = equivResultFor(equiv1, ImmutableList.<Item>of());
        
        updater.handle(equivResult, null);
        updater.handle(noEquivalences, null);
        
        verify(lookupWriter).writeLookup(argThat(is(contentRef)), argThat(hasItems(equiv1Ref,equiv2Ref)), argThat(is(publishers)));
        verify(lookupWriter, never()).writeLookup(argThat(is(equiv1Ref)), argThat(any(Iterable.class)), argThat(is(publishers)));
    }

    @Test
    public void testWritesLookupsForItemWhichWasSeenAsEquivalentButDoesntAssertAnyEquivalencesWhenCacheTimesOut() throws InterruptedException {
        
        Duration cacheDuration = new Duration(5);
        LookupWritingEquivalenceHandler<Item> updater = new LookupWritingEquivalenceHandler<Item>(lookupWriter, publishers, cacheDuration);
        
        final EquivalenceResult<Item> equivResult1 = equivResultFor(content, ImmutableList.of(equiv1,equiv2));
        final EquivalenceResult<Item> equivResult2 = equivResultFor(equiv1, ImmutableList.<Item>of(content));
        
        updater.handle(equivResult1, null);
        Thread.sleep(cacheDuration.getMillis()*2);
        updater.handle(equivResult2, null);

        verify(lookupWriter).writeLookup(argThat(is(contentRef)), argThat(hasItems(equiv1Ref,equiv2Ref)), argThat(is(publishers)));
        verify(lookupWriter).writeLookup(argThat(is(equiv1Ref)), argThat(hasItems(contentRef)), argThat(is(publishers)));
        
    }
    
    private EquivalenceResult<Item> equivResultFor(Item content, Iterable<Item> equivalents) {
        Multimap<Publisher, ScoredCandidate<Item>> strong = Multimaps.transformValues(Multimaps.index(equivalents, TO_PUBLISHER),RANDOM_SCORE);
        return new EquivalenceResult<Item>(content, ImmutableList.<ScoredCandidates<Item>>of(), null, strong , null);
    }
    
    private static final Function<Content, Publisher> TO_PUBLISHER = new Function<Content, Publisher>() {

        @Override
        public Publisher apply(Content input) {
            return input.getPublisher();
        }
    };

    private static final Function<Item, ScoredCandidate<Item>> RANDOM_SCORE = new Function<Item, ScoredCandidate<Item>>() {
        @Override
        public ScoredCandidate<Item> apply(Item input) {
            //Chosen by fair dice roll / 10. Guaranteed to be random.
            return ScoredCandidate.valueOf(input, Score.valueOf(0.4));
        }
    };
}

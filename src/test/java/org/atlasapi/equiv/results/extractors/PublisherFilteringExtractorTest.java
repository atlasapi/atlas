package org.atlasapi.equiv.results.extractors;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.atlasapi.equiv.results.description.DefaultDescription;
import org.atlasapi.equiv.results.filters.PublisherFilter;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidate;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class PublisherFilteringExtractorTest {

    @Test
    public void testFiltersUnacceptablePublishers() {
        
        PublisherFilter<Item> filter = new PublisherFilter<Item>();
        
        List<ScoredCandidate<Item>> paScore = ImmutableList.of(scoreOneFor(Publisher.PA));
        
        assertFalse(filter.apply(paScore, itemWithPublisher(Publisher.PA), new DefaultDescription()).iterator().hasNext());
        assertTrue(filter.apply(paScore, itemWithPublisher(Publisher.BBC), new DefaultDescription()).iterator().hasNext());
        assertTrue(filter.apply(paScore, itemWithPublisher(Publisher.C4_PMLSD), new DefaultDescription()).iterator().hasNext());
        
        List<ScoredCandidate<Item>> BbcScore = ImmutableList.of(scoreOneFor(Publisher.BBC));
        assertFalse(filter.apply(BbcScore, itemWithPublisher(Publisher.C4_PMLSD), new DefaultDescription()).iterator().hasNext());
        assertTrue(filter.apply(BbcScore, itemWithPublisher(Publisher.SEESAW), new DefaultDescription()).iterator().hasNext());
        
        List<ScoredCandidate<Item>> dmScore = ImmutableList.of(scoreOneFor(Publisher.DAILYMOTION));
        assertTrue(filter.apply(dmScore, itemWithPublisher(Publisher.C4_PMLSD), new DefaultDescription()).iterator().hasNext());
        
    }

    private ScoredCandidate<Item> scoreOneFor(Publisher pub) {
        return ScoredCandidate.valueOf(itemWithPublisher(pub), Score.valueOf(1.0));
    }

    private Item itemWithPublisher(Publisher pub) {
        return new Item("uri", "curie", pub);
    }
    
}

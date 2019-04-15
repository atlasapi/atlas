package org.atlasapi.equiv.results.extractors;

import com.google.common.collect.ImmutableList;
import org.atlasapi.equiv.results.description.DefaultDescription;
import org.atlasapi.equiv.results.filters.PublisherFilter;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidate;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResult;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class PublisherFilteringExtractorTest {

    @Test
    public void testFiltersUnacceptablePublishers() {

        EquivToTelescopeResult equivToTelescopeResult =
                EquivToTelescopeResult.create("id", "publisher");
        
        PublisherFilter<Item> filter = new PublisherFilter<Item>();
        
        List<ScoredCandidate<Item>> paScore = ImmutableList.of(scoreOneFor(Publisher.PA));
        
        assertFalse(filter.apply(
                paScore,
                itemWithPublisher(Publisher.PA),
                new DefaultDescription(),
                equivToTelescopeResult
        ).iterator().hasNext());

        assertTrue(filter.apply(
                paScore,
                itemWithPublisher(Publisher.BBC),
                new DefaultDescription(),
                equivToTelescopeResult
        ).iterator().hasNext());

        assertTrue(filter.apply(
                paScore,
                itemWithPublisher(Publisher.C4_PMLSD),
                new DefaultDescription(),
                equivToTelescopeResult).iterator().hasNext()
        );
        
        List<ScoredCandidate<Item>> BbcScore = ImmutableList.of(scoreOneFor(Publisher.BBC));
        assertFalse(filter.apply(
                BbcScore,
                itemWithPublisher(Publisher.C4_PMLSD),
                new DefaultDescription(),
                equivToTelescopeResult
        ).iterator().hasNext());

        assertTrue(filter.apply(
                BbcScore,
                itemWithPublisher(Publisher.SEESAW),
                new DefaultDescription(),
                equivToTelescopeResult
        ).iterator().hasNext());
        
        List<ScoredCandidate<Item>> dmScore = ImmutableList.of(scoreOneFor(Publisher.DAILYMOTION));
        assertTrue(filter.apply(
                dmScore,
                itemWithPublisher(Publisher.C4_PMLSD),
                new DefaultDescription(),
                equivToTelescopeResult
        ).iterator().hasNext());
    }

    private ScoredCandidate<Item> scoreOneFor(Publisher pub) {
        return ScoredCandidate.valueOf(itemWithPublisher(pub), Score.valueOf(1.0));
    }

    private Item itemWithPublisher(Publisher pub) {
        Item item = new Item("uri", "curie", pub);
        item.setId(1L);
        return item;
    }
    
}

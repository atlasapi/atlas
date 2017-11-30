package org.atlasapi.equiv.results.extractors;

import java.util.Set;

import junit.framework.TestCase;

import org.atlasapi.equiv.results.description.DefaultDescription;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidate;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResults;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.junit.Test;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;

public class PercentThresholdEquivalenceExtractorTest extends TestCase {

    private final EquivToTelescopeResults equivToTelescopeResults =
            EquivToTelescopeResults.create("id", "publisher");

    @Test
    public void testExtractsItemWith90PercentOfTotalWithNegatives() {

        PercentThresholdEquivalenceExtractor<Item> extractor = PercentThresholdEquivalenceExtractor.<Item>moreThanPercent(90);
        
        ScoredCandidate<Item> strong = ScoredCandidate.valueOf(new Item("test1","cur1",Publisher.BBC), Score.valueOf(0.5));
        Set<ScoredCandidate<Item>> extractSet = extractor.extract(ImmutableList.<ScoredCandidate<Item>>of(
                strong,
                ScoredCandidate.valueOf(new Item("test2","cur2",Publisher.BBC), Score.valueOf(-0.5)),
                ScoredCandidate.valueOf(new Item("test3","cur3",Publisher.BBC), Score.valueOf(-0.5)),
                ScoredCandidate.valueOf(new Item("test4","cur4",Publisher.BBC), Score.valueOf(-0.5))
        ), null, new DefaultDescription(), equivToTelescopeResults);

        ScoredCandidate<Item> extract = extractSet.iterator().next();

        assertTrue("Nothing strong extracted", !extractSet.isEmpty());
        assertEquals(extract, strong);
        
    }

    @Test
    public void testDoesntExtractItemWhenAllNegative() {

        PercentThresholdEquivalenceExtractor<Item> extractor = PercentThresholdEquivalenceExtractor.<Item>moreThanPercent(90);
        
        Set<ScoredCandidate<Item>> extractSet = extractor.extract(ImmutableList.<ScoredCandidate<Item>>of(
                ScoredCandidate.valueOf(new Item("test1","cur1",Publisher.BBC), Score.valueOf(-0.5)),
                ScoredCandidate.valueOf(new Item("test2","cur2",Publisher.BBC), Score.valueOf(-0.5)),
                ScoredCandidate.valueOf(new Item("test3","cur3",Publisher.BBC), Score.valueOf(-0.5)),
                ScoredCandidate.valueOf(new Item("test4","cur4",Publisher.BBC), Score.valueOf(-0.5))
        ), null, new DefaultDescription(), equivToTelescopeResults);
        
        assertTrue("Something strong extracted", extractSet.isEmpty());
    }

}

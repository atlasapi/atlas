package org.atlasapi.equiv.results.extractors;

import com.google.common.collect.ImmutableList;
import org.atlasapi.equiv.results.description.DefaultDescription;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidate;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResult;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.junit.Test;

import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class PercentThresholdAboveNextBestMatchEquivalenceExtractorTest {

    private final EquivToTelescopeResult equivToTelescopeResult =
            EquivToTelescopeResult.create("id", "publisher");

    @Test
    public void testExtractsWhenStrongBeatsNextBestByThreshold() {

        PercentThresholdAboveNextBestMatchEquivalenceExtractor<Item> extractor = PercentThresholdAboveNextBestMatchEquivalenceExtractor.<Item>atLeastNTimesGreater(2);
        
        ScoredCandidate<Item> strong = ScoredCandidate.valueOf(new Item("test1","cur1",Publisher.BBC), Score.valueOf(1.0));
        Set<ScoredCandidate<Item>> extractSet = extractor.extract(ImmutableList.<ScoredCandidate<Item>>of(
                strong,
                ScoredCandidate.valueOf(new Item("test2","cur2",Publisher.BBC), Score.valueOf(0.5))
        ), null, new DefaultDescription(),
                equivToTelescopeResult);
        ScoredCandidate<Item> extract = extractSet.iterator().next();

        assertTrue("Strong extracted", !extractSet.isEmpty());
        assertEquals(extract, strong);
        
    } 
    
    @Test
    public void testDoesntExtractWhenStrongBeatsNextBestByThreshold() {

        PercentThresholdAboveNextBestMatchEquivalenceExtractor<Item> extractor = PercentThresholdAboveNextBestMatchEquivalenceExtractor.<Item>atLeastNTimesGreater(2);
        
        ScoredCandidate<Item> strong = ScoredCandidate.valueOf(new Item("test1","cur1",Publisher.BBC), Score.valueOf(1.0));
        Set<ScoredCandidate<Item>> extractSet = extractor.extract(
                ImmutableList.<ScoredCandidate<Item>>of(
                        strong,
                        ScoredCandidate.valueOf(
                                new Item("test2","cur2",Publisher.BBC),
                                Score.valueOf(0.6))
                ),
                null,
                new DefaultDescription(),
                equivToTelescopeResult);
        assertFalse("Strong should not be extracted", !extractSet.isEmpty());
    } 
    
    @Test
    public void testExtractsWhenOnlyOneCandidate() {

        PercentThresholdAboveNextBestMatchEquivalenceExtractor<Item> extractor = PercentThresholdAboveNextBestMatchEquivalenceExtractor.<Item>atLeastNTimesGreater(2);
        
        ScoredCandidate<Item> strong = ScoredCandidate.valueOf(new Item("test1","cur1",Publisher.BBC), Score.valueOf(1.0));
        Set<ScoredCandidate<Item>> extractSet = extractor.extract(
                ImmutableList.<ScoredCandidate<Item>>of(strong),
                null,
                new DefaultDescription(),
                equivToTelescopeResult
        );

        ScoredCandidate<Item> extract = extractSet.iterator().next();
        assertTrue("Strong extracted", !extractSet.isEmpty());
        assertEquals(extract, strong);
    } 
}

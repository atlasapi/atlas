package org.atlasapi.equiv.results.extractors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.atlasapi.equiv.results.description.DefaultDescription;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidate;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.junit.Test;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;


public class PercentThresholdAboveNextBestMatchEquivalenceExtractorTest {

    @Test
    public void testExtractsWhenStrongBeatsNextBestByThreshold() {

        PercentThresholdAboveNextBestMatchEquivalenceExtractor<Item> extractor = PercentThresholdAboveNextBestMatchEquivalenceExtractor.<Item>atLeastNTimesGreater(2);
        
        ScoredCandidate<Item> strong = ScoredCandidate.valueOf(new Item("test1","cur1",Publisher.BBC), Score.valueOf(1.0));
        Optional<ScoredCandidate<Item>> extract = extractor.extract(ImmutableList.<ScoredCandidate<Item>>of(
                strong,
                ScoredCandidate.valueOf(new Item("test2","cur2",Publisher.BBC), Score.valueOf(0.5))
        ), null, new DefaultDescription());
        
        assertTrue("Strong extracted", extract.isPresent());
        assertEquals(extract.get(), strong);
        
    } 
    
    @Test
    public void testDoesntExtractWhenStrongBeatsNextBestByThreshold() {

        PercentThresholdAboveNextBestMatchEquivalenceExtractor<Item> extractor = PercentThresholdAboveNextBestMatchEquivalenceExtractor.<Item>atLeastNTimesGreater(2);
        
        ScoredCandidate<Item> strong = ScoredCandidate.valueOf(new Item("test1","cur1",Publisher.BBC), Score.valueOf(1.0));
        Optional<ScoredCandidate<Item>> extract = extractor.extract(ImmutableList.<ScoredCandidate<Item>>of(
                strong,
                ScoredCandidate.valueOf(new Item("test2","cur2",Publisher.BBC), Score.valueOf(0.6))
        ), null, new DefaultDescription());
        
        assertFalse("Strong should not be extracted", extract.isPresent());
    } 
    
    @Test
    public void testExtractsWhenOnlyOneCandidate() {

        PercentThresholdAboveNextBestMatchEquivalenceExtractor<Item> extractor = PercentThresholdAboveNextBestMatchEquivalenceExtractor.<Item>atLeastNTimesGreater(2);
        
        ScoredCandidate<Item> strong = ScoredCandidate.valueOf(new Item("test1","cur1",Publisher.BBC), Score.valueOf(1.0));
        Optional<ScoredCandidate<Item>> extract = extractor.extract(ImmutableList.<ScoredCandidate<Item>>of(strong), null, new DefaultDescription());
        
        assertTrue("Strong extracted", extract.isPresent());
        assertEquals(extract.get(), strong);
    } 
}

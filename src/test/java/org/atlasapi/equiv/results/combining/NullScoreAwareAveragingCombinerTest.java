package org.atlasapi.equiv.results.combining;

import java.util.List;

import junit.framework.TestCase;

import org.atlasapi.equiv.results.description.DefaultDescription;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.equiv.scorers.SequenceContainerScorer;
import org.atlasapi.equiv.scorers.TitleMatchingItemScorer;
import org.atlasapi.equiv.scorers.TitleSubsetBroadcastItemScorer;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class NullScoreAwareAveragingCombinerTest extends TestCase {
    
    private final NullScoreAwareAveragingCombiner<Item> combiner = NullScoreAwareAveragingCombiner.get();

    private final Item equivalent1 = target("equivalent1", "Equivalent1", Publisher.BBC);
    private final Item equivalent2 = target("equivalent2", "Equivalent2", Publisher.C4);
    private final Item equivalent3 = target("equivalent3", "Equivalent3", Publisher.FIVE);
    
    private Item target(String name, String title, Publisher publisher) {
        Item target = new Item(name+"Uri", name+"Curie", publisher);
        target.setTitle("Test " + title);
        return target;
    }

    @Test
    public void testCombine() {
        
        List<ScoredCandidates<Item>> scores = ImmutableList.of(
                DefaultScoredCandidates.<Item>fromSource("source2")
                    .addEquivalent(equivalent1, Score.valueOf(5.0))
                    .addEquivalent(equivalent2, Score.NULL_SCORE)
                    .addEquivalent(equivalent3, Score.valueOf(5.0))
                    .build(),
                DefaultScoredCandidates.<Item>fromSource("source1")
                    .addEquivalent(equivalent1, Score.valueOf(5.0))
                    .addEquivalent(equivalent2, Score.valueOf(5.0))
                    .addEquivalent(equivalent3, Score.valueOf(5.0))
                    .addEquivalent(equivalent1, Score.valueOf(5.0))
                    .build(),
                DefaultScoredCandidates.<Item>fromSource("source3")
                    .addEquivalent(equivalent3, Score.valueOf(5.0))
                    .addEquivalent(equivalent1, Score.NULL_SCORE)
                    .build()
        );
        
        ScoredCandidates<Item> combined = combiner.combine(scores, new DefaultDescription());
        
        assertEquals(Score.valueOf(5.0), combined.candidates().get(equivalent3));
        assertEquals(Score.valueOf(7.5), combined.candidates().get(equivalent1));
        assertEquals(Score.valueOf(5.0), combined.candidates().get(equivalent2));
    }

    @Test
    public void testCombineNulls() {
        
        List<ScoredCandidates<Item>> scores = ImmutableList.of(
                DefaultScoredCandidates.<Item>fromSource("source2")
                    .addEquivalent(equivalent1, Score.NULL_SCORE)
                    .addEquivalent(equivalent2, Score.NULL_SCORE)
                    .build(),
                DefaultScoredCandidates.<Item>fromSource("source1")
                    .addEquivalent(equivalent2, Score.valueOf(5.0))
                    .addEquivalent(equivalent3, Score.valueOf(5.0))
                    .build(),
                DefaultScoredCandidates.<Item>fromSource("source3")
                    .addEquivalent(equivalent3, Score.NULL_SCORE)
                    .build()
        );
        
        ScoredCandidates<Item> combined = combiner.combine(scores, new DefaultDescription());

        assertEquals(Score.valueOf(5.0), combined.candidates().get(equivalent3));
        assertEquals(Score.NULL_SCORE,   combined.candidates().get(equivalent1));
        assertEquals(Score.valueOf(5.0), combined.candidates().get(equivalent2));
        
    }
    
    private final Item equivalent4 = target("equivalent4", "Equivalent4", Publisher.BBC);
    private final Item equivalent5 = target("equivalent5", "Equivalent5", Publisher.BBC);
    private final Item equivalent6 = target("equivalent6", "Equivalent6", Publisher.BBC);

    @Test
    public void testCombiningScoresForSamePublisherAveragesOverHighestNumberOfSources() {
        /* Equivalent 6 scores in all sources so averaging for all items should be over 3 
         * as they're all from the same publisher.
         */
        List<ScoredCandidates<Item>> scores = ImmutableList.of(
                DefaultScoredCandidates.<Item>fromSource("source2")
                    .addEquivalent(equivalent4, Score.valueOf(5.0))
                    .addEquivalent(equivalent5, Score.NULL_SCORE)
                    .addEquivalent(equivalent6, Score.valueOf(5.0))
                    .build(),
                DefaultScoredCandidates.<Item>fromSource("source1")
                    .addEquivalent(equivalent4, Score.valueOf(5.0))
                    .addEquivalent(equivalent5, Score.valueOf(6.0))
                    .addEquivalent(equivalent6, Score.valueOf(5.0))
                    .addEquivalent(equivalent4, Score.valueOf(5.0))
                    .build(),
                DefaultScoredCandidates.<Item>fromSource("source3")
                    .addEquivalent(equivalent6, Score.valueOf(5.0))
                    .addEquivalent(equivalent4, Score.NULL_SCORE)
                    .build()
        );
        
        ScoredCandidates<Item> combined = combiner.combine(scores, new DefaultDescription());
        
        assertEquals(Score.valueOf(5.0), combined.candidates().get(equivalent6));
        assertEquals(Score.valueOf(5.0), combined.candidates().get(equivalent4));
        assertEquals(Score.valueOf(2.0), combined.candidates().get(equivalent5));
        
    }

    @Test
    public void testIgnoresTitleSubsetIfThereIsTitleMatcherScore() {
        List<ScoredCandidates<Item>> scores = ImmutableList.of(
                DefaultScoredCandidates.<Item>fromSource(TitleMatchingItemScorer.NAME)
                        .addEquivalent(equivalent4, Score.valueOf(10.0))
                        .addEquivalent(equivalent5, Score.NULL_SCORE)
                        .addEquivalent(equivalent6, Score.valueOf(10.0))
                        .build(),
                DefaultScoredCandidates.<Item>fromSource(TitleSubsetBroadcastItemScorer.NAME)
                        .addEquivalent(equivalent4, Score.valueOf(100.0))
                        .addEquivalent(equivalent5, Score.valueOf(100.0))
                        .addEquivalent(equivalent6, Score.valueOf(100.0))
                        .build(),
                DefaultScoredCandidates.<Item>fromSource("Broadcast-Alias")
                        .addEquivalent(equivalent4, Score.valueOf(2.0))
                        .addEquivalent(equivalent5, Score.valueOf(2.0))
                        .addEquivalent(equivalent6, Score.valueOf(2.0))
                        .build()
        );

        ScoredCandidates<Item> combined = combiner.combine(scores, new DefaultDescription());

        assertEquals(Score.valueOf(6.0), combined.candidates().get(equivalent6));
        assertEquals(Score.valueOf(6.0), combined.candidates().get(equivalent4));
        assertEquals(Score.valueOf(1.0), combined.candidates().get(equivalent5));

    }
}

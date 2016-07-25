package org.atlasapi.equiv.results;

import java.util.List;

import org.atlasapi.equiv.results.combining.AddingEquivalenceCombiner;
import org.atlasapi.equiv.results.combining.ScoreCombiner;
import org.atlasapi.equiv.results.description.DefaultDescription;
import org.atlasapi.equiv.results.extractors.EquivalenceExtractor;
import org.atlasapi.equiv.results.extractors.TopEquivalenceExtractor;
import org.atlasapi.equiv.results.filters.AlwaysTrueFilter;
import org.atlasapi.equiv.results.filters.EquivalenceFilter;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class DefaultEquivalenceResultBuilderTest {

    private final ScoreCombiner<Item> combiner = AddingEquivalenceCombiner.create();
    private final EquivalenceFilter<Item> filter = AlwaysTrueFilter.get();
    private final EquivalenceExtractor<Item> extractor = TopEquivalenceExtractor.create();
    private final DefaultEquivalenceResultBuilder resultBuilder = new DefaultEquivalenceResultBuilder(combiner, filter, extractor);

    @Test
    public void checkEquivalatesToSeveralPa() {
        Item item = new Item();
        item.setPublisher(Publisher.ARQIVA);
        item.setCanonicalUri("target");
        List<ScoredCandidates<Item>> equivalents = getScoredCandidates(5.0, 5.0);
        EquivalenceResult equivalenceResult = resultBuilder.resultFor(
                item,
                equivalents,
                new DefaultDescription()
        );

        assertTrue(equivalenceResult.strongEquivalences().values().size() == 2);
    }

    @Test
    public void checkDoesntEquivalateToSeveralPaWithDifferentScores() {
        Item item = new Item();
        item.setPublisher(Publisher.ARQIVA);
        item.setCanonicalUri("target");
        List<ScoredCandidates<Item>> equivalents = getScoredCandidates(4.5, 5.0);
        EquivalenceResult equivalenceResult = resultBuilder.resultFor(
                item,
                equivalents,
                new DefaultDescription()
        );

        assertTrue(equivalenceResult.strongEquivalences().values().size() == 1);
    }

    private List<ScoredCandidates<Item>> getScoredCandidates(double score1, double score2) {
        Item candidate1 = new Item();
        candidate1.setPublisher(Publisher.PA);
        candidate1.setCanonicalUri("candidate1");
        Item candidate2 = new Item();
        candidate2.setPublisher(Publisher.PA);
        candidate2.setCanonicalUri("candidate2");

        return ImmutableList.of(
                DefaultScoredCandidates.<Item>fromSource("A Source")
                        .addEquivalent(candidate1, Score.valueOf(score1))
                        .addEquivalent(candidate2, Score.valueOf(score2))
                        .build()
        );
    }

}
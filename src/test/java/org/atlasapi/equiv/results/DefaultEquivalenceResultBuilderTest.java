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
        List<ScoredCandidates<Item>> equivalents = getScoredCandidates(5.0, 5.0, 4.5, 4.5);
        EquivalenceResult equivalenceResult = resultBuilder.resultFor(
                item,
                equivalents,
                new DefaultDescription()
        );

        assertTrue(equivalenceResult.strongEquivalences().values().size() == 4);
    }

    @Test
    public void checkEmptyEquivalates() {
        Item item = new Item();
        item.setPublisher(Publisher.ARQIVA);
        item.setCanonicalUri("target");
        List<ScoredCandidates<Item>> equivalents = ImmutableList.of();
        EquivalenceResult equivalenceResult = resultBuilder.resultFor(
                item,
                equivalents,
                new DefaultDescription()
        );

        assertTrue(equivalenceResult.strongEquivalences().values().size() == 0);
    }

    @Test
    public void checkOneEquivalates() {
        Item item = new Item();
        item.setPublisher(Publisher.ARQIVA);
        item.setCanonicalUri("target");
        Item candidate = new Item();
        candidate.setPublisher(Publisher.PA);
        candidate.setCanonicalUri("candidate");
        List<ScoredCandidates<Item>> equivalents = ImmutableList.of(DefaultScoredCandidates.<Item>fromSource("A Source")
                .addEquivalent(candidate, Score.valueOf(0.5))
                .build());
        EquivalenceResult equivalenceResult = resultBuilder.resultFor(
                item,
                equivalents,
                new DefaultDescription()
        );

        assertTrue(equivalenceResult.strongEquivalences().values().size() == 1);
    }

    @Test
    public void checkDoesntEquivalateToSeveralPaIfNaN() {
        Item item = new Item();
        item.setPublisher(Publisher.ARQIVA);
        item.setCanonicalUri("target");
        List<ScoredCandidates<Item>> equivalents = getScoredCandidates(Double.NaN, 5.0, 5.0, 5.0);
        EquivalenceResult equivalenceResult = resultBuilder.resultFor(
                item,
                equivalents,
                new DefaultDescription()
        );

        assertTrue(equivalenceResult.strongEquivalences().values().size() == 1);
    }

    @Test
    public void checkEquivalatesTwoItemsWithSameScore() {
        Item item = new Item();
        item.setPublisher(Publisher.ARQIVA);
        item.setCanonicalUri("target");
        Item item1 = new Item();
        item1.setPublisher(Publisher.PA);
        item1.setCanonicalUri("candidate1");
        Item item2 = new Item();
        item2.setPublisher(Publisher.PA);
        item2.setCanonicalUri("candidate2");
        List<ScoredCandidates<Item>> equivalents = ImmutableList.of(DefaultScoredCandidates.<Item>fromSource("A Source")
                .addEquivalent(item1, Score.valueOf(1.01))
                .addEquivalent(item2, Score.valueOf(1.01))
                .build());
        EquivalenceResult equivalenceResult = resultBuilder.resultFor(
                item,
                equivalents,
                new DefaultDescription()
        );

        assertTrue(equivalenceResult.strongEquivalences().values().size() == 1);
    }
    @Test
    public void checkDoesntEquivalatesTwoItemsWithDifferentScore() {
        Item item = new Item();
        item.setPublisher(Publisher.ARQIVA);
        item.setCanonicalUri("target");
        Item item1 = new Item();
        item1.setPublisher(Publisher.PA);
        item1.setCanonicalUri("candidate1");
        Item item2 = new Item();
        item2.setPublisher(Publisher.PA);
        item2.setCanonicalUri("candidate2");
        List<ScoredCandidates<Item>> equivalents = ImmutableList.of(DefaultScoredCandidates.<Item>fromSource("A Source")
                .addEquivalent(item1, Score.valueOf(1.01))
                .addEquivalent(item2, Score.valueOf(1.3))
                .build());
        EquivalenceResult equivalenceResult = resultBuilder.resultFor(
                item,
                equivalents,
                new DefaultDescription()
        );

        assertTrue(equivalenceResult.strongEquivalences().values().size() == 1);
    }


    @Test
    public void checkDoesntEquivalateToSeveralPaWithDifferentScores() {
        Item item = new Item();
        item.setPublisher(Publisher.ARQIVA);
        item.setCanonicalUri("target");
        List<ScoredCandidates<Item>> equivalents = getScoredCandidates(1, 2, 2, 2);
        EquivalenceResult equivalenceResult = resultBuilder.resultFor(
                item,
                equivalents,
                new DefaultDescription()
        );

        assertTrue(equivalenceResult.strongEquivalences().values().size() == 1);
    }

    private List<ScoredCandidates<Item>> getScoredCandidates(double score1, double score2, double score3, double score4) {
        Item candidate1 = new Item();
        candidate1.setPublisher(Publisher.PA);
        candidate1.setCanonicalUri("candidate1");
        Item candidate2 = new Item();
        candidate2.setPublisher(Publisher.PA);
        candidate2.setCanonicalUri("candidate2");
        Item candidate3 = new Item();
        candidate3.setPublisher(Publisher.PA);
        candidate3.setCanonicalUri("candidate3");
        Item candidate4 = new Item();
        candidate4.setPublisher(Publisher.PA);
        candidate4.setCanonicalUri("candidate4");

        return ImmutableList.of(
                DefaultScoredCandidates.<Item>fromSource("A Source")
                        .addEquivalent(candidate1, Score.valueOf(score1))
                        .addEquivalent(candidate2, Score.valueOf(score2))
                        .addEquivalent(candidate3, Score.valueOf(score3))
                        .addEquivalent(candidate4, Score.valueOf(score4))
                        .build()
        );
    }

}
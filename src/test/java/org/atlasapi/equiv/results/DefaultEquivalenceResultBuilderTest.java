package org.atlasapi.equiv.results;

import java.util.Date;
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
import org.atlasapi.equiv.results.scores.ScoredCandidate;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Series;
import org.atlasapi.media.entity.Version;

import com.google.common.base.Equivalence;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.joda.time.DateTime;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class DefaultEquivalenceResultBuilderTest {

    private final ScoreCombiner<Item> combiner = AddingEquivalenceCombiner.create();
    private final EquivalenceFilter<Item> filter = AlwaysTrueFilter.get();
    private final EquivalenceExtractor<Item> extractor = TopEquivalenceExtractor.create();
    private final DefaultEquivalenceResultBuilder resultBuilder = new DefaultEquivalenceResultBuilder(combiner, filter, extractor);

    @Test
    public void checkDoesNotEquivalateToSeveralPaWithoutBroadcasts() {
        Item item = new Item();
        item.setPublisher(Publisher.ARQIVA);
        item.setCanonicalUri("target");
        List<ScoredCandidates<Item>> equivalents = getScoredCandidates(5.0, 5.0, 4.7, 4.7);
        EquivalenceResult equivalenceResult = resultBuilder.resultFor(
                item,
                equivalents,
                new DefaultDescription()
        );

        assertTrue(equivalenceResult.strongEquivalences().values().size() == 1);
    }

    @Test
    public void checkTopCandidateTakenAloneIfSeriesOrBrand() {
        Item item = itemWithBroadcast(
                new DateTime().withHourOfDay(2),
                new DateTime().withHourOfDay(4)
        );
        item.setPublisher(Publisher.ARQIVA);
        item.setCanonicalUri("target");

        Brand candidate1 = brandWithBroadcast(
                new DateTime().withHourOfDay(2),
                new DateTime().withHourOfDay(6)
        );
        candidate1.setPublisher(Publisher.PA);
        candidate1.setCanonicalUri("candidate1");

        Item candidate2 = itemWithBroadcast(
                new DateTime().withHourOfDay(2),
                new DateTime().withHourOfDay(4)
        );
        candidate2.setPublisher(Publisher.PA);
        candidate2.setCanonicalUri("candidate2");

        List<ScoredCandidates<Identified>> equivalents = ImmutableList.of(
                DefaultScoredCandidates.<Identified>fromSource("A Source")
                        .addEquivalent(candidate1, Score.valueOf(5.0))
                        .addEquivalent(candidate2, Score.valueOf(4.8))
                        .build()
        );

        EquivalenceResult equivalenceResult = resultBuilder.resultFor(
                item,
                equivalents,
                new DefaultDescription()
        );
        assertTrue(equivalenceResult.strongEquivalences().values().size() == 1);
    }

    @Test
    public void checkDoesEquivalateToSeveralPaWithInclusiveBroadcasts() {

        EquivalenceResult equivalenceResult = itemsWithBroadcastAndScores(
                new DateTime().withHourOfDay(1),
                new DateTime().withHourOfDay(6),
                new DateTime().withHourOfDay(4),
                new DateTime().withHourOfDay(2),
                new DateTime().withHourOfDay(5),
                new DateTime().withHourOfDay(6),
                5.0,
                4.8
        );
        assertTrue(equivalenceResult.strongEquivalences().values().size() == 2);
    }

    @Test
    public void checkDoesEquivalateToSeveralPaWithBoundaryBroadcasts() {

        EquivalenceResult equivalenceResult = itemsWithBroadcastAndScores(
                new DateTime().withHourOfDay(4),
                new DateTime().withHourOfDay(5),
                new DateTime().withHourOfDay(4),
                new DateTime().withHourOfDay(4),
                new DateTime().withHourOfDay(5),
                new DateTime().withHourOfDay(5),
                5.0,
                4.8
        );
        assertTrue(equivalenceResult.strongEquivalences().values().size() == 2);
    }

    @Test
    public void checkWontEquivalateToBothWithOverlappingScores() {
        EquivalenceResult equivalenceResult = itemsWithBroadcastAndScores(
                new DateTime().withHourOfDay(2),
                new DateTime().withHourOfDay(4),
                new DateTime().withHourOfDay(2),
                new DateTime().withHourOfDay(5),
                new DateTime().withHourOfDay(4),
                new DateTime().withHourOfDay(6),
                5.0,
                4.8
        );
        assertTrue(equivalenceResult.strongEquivalences().values().size() == 1);
    }

    @Test
    public void checkWontEquivalateToBrands() {
        Item item = new Item();
        item.setPublisher(Publisher.ARQIVA);
        item.setCanonicalUri("target");

        Brand candidate1 = brandWithBroadcast(
                new DateTime().withHourOfDay(2),
                new DateTime().withHourOfDay(6)
        );
        candidate1.setPublisher(Publisher.PA);
        candidate1.setCanonicalUri("candidate1");

        Item candidate2 = itemWithBroadcast(
                new DateTime().withHourOfDay(4),
                new DateTime().withHourOfDay(5)
        );
        candidate2.setPublisher(Publisher.PA);
        candidate2.setCanonicalUri("candidate2");

        List<ScoredCandidates<Identified>> equivalents = ImmutableList.of(
                DefaultScoredCandidates.<Identified>fromSource("A Source")
                        .addEquivalent(candidate1, Score.valueOf(5.0))
                        .addEquivalent(candidate2, Score.valueOf(4.8))
                        .build()
        );

        EquivalenceResult equivalenceResult = resultBuilder.resultFor(
                item,
                equivalents,
                new DefaultDescription()
        );
        assertTrue(equivalenceResult.strongEquivalences().values().size() == 1);
    }

    @Test
    public void checkWontEquivalateToSeries() {
        Item item = new Item();
        item.setPublisher(Publisher.ARQIVA);
        item.setCanonicalUri("target");

        Series candidate1 = seriesWithBroadcast(
                new DateTime().withHourOfDay(2),
                new DateTime().withHourOfDay(6)
        );
        candidate1.setPublisher(Publisher.PA);
        candidate1.setCanonicalUri("candidate1");


        Item candidate2 = itemWithBroadcast(
                new DateTime().withHourOfDay(4),
                new DateTime().withHourOfDay(5)
        );
        candidate2.setPublisher(Publisher.PA);
        candidate2.setCanonicalUri("candidate2");

        List<ScoredCandidates<Identified>> equivalents = ImmutableList.of(
                DefaultScoredCandidates.<Identified>fromSource("A Source")
                        .addEquivalent(candidate1, Score.valueOf(5.0))
                        .addEquivalent(candidate2, Score.valueOf(4.8))
                        .build()
        );

        EquivalenceResult equivalenceResult = resultBuilder.resultFor(
                item,
                equivalents,
                new DefaultDescription()
        );
        assertTrue(equivalenceResult.strongEquivalences().values().size() == 1);
    }

    @Test
    public void checkWillOnlyTakeBroadcastMatchingCandidatesNotAll() {
        EquivalenceResult equivalenceResult = itemsWithBroadcastAndScores(
                new DateTime().withHourOfDay(2),
                new DateTime().withHourOfDay(4),
                new DateTime().withHourOfDay(2),
                new DateTime().withHourOfDay(3),
                new DateTime().withHourOfDay(9),
                new DateTime().withHourOfDay(4),
                new DateTime().withHourOfDay(6),
                new DateTime().withHourOfDay(9).withMinuteOfHour(15),
                5.0,
                4.8,
                4.9
        );
        assertTrue(equivalenceResult.strongEquivalences().values().size() == 1);
    }

    private EquivalenceResult itemsWithBroadcastAndScores(
            DateTime targetStart,
            DateTime targetEnd,
            DateTime startTime1,
            DateTime startTime2,
            DateTime startTime3,
            DateTime endTime1,
            DateTime endTime2,
            DateTime endTime3,
            Double score1,
            Double score2,
            Double score3
    ) {
        Item item = itemWithBroadcast(targetStart, targetEnd);
        item.setPublisher(Publisher.ARQIVA);
        item.setCanonicalUri("target");

        Item candidate1 = itemWithBroadcast(startTime1, endTime1);
        candidate1.setPublisher(Publisher.PA);
        candidate1.setCanonicalUri("candidate1");


        Item candidate2 = itemWithBroadcast(startTime2, endTime2);
        candidate2.setPublisher(Publisher.PA);
        candidate2.setCanonicalUri("candidate2");

        Item candidate3 = itemWithBroadcast(startTime3, endTime3);
        candidate3.setPublisher(Publisher.PA);
        candidate3.setCanonicalUri("candidate3");

        List<ScoredCandidates<Item>> equivalents = ImmutableList.of(
                DefaultScoredCandidates.<Item>fromSource("A Source")
                        .addEquivalent(candidate1, Score.valueOf(score1))
                        .addEquivalent(candidate2, Score.valueOf(score2))
                        .addEquivalent(candidate3, Score.valueOf(score3))
                        .build()
        );

        EquivalenceResult equivalenceResult = resultBuilder.resultFor(
                item,
                equivalents,
                new DefaultDescription()
        );
        return equivalenceResult;
    }

    private EquivalenceResult itemsWithBroadcastAndScores(
            DateTime targetStart,
            DateTime targetEnd,
            DateTime startTime1,
            DateTime startTime2,
            DateTime endTime1,
            DateTime endTime2,
            Double score1,
            Double score2
    ) {
        Item item = itemWithBroadcast(targetStart, targetEnd);
        item.setPublisher(Publisher.ARQIVA);
        item.setCanonicalUri("target");

        Item candidate1 = itemWithBroadcast(startTime1, endTime1);
        candidate1.setPublisher(Publisher.PA);
        candidate1.setCanonicalUri("candidate1");


        Item candidate2 = itemWithBroadcast(startTime2, endTime2);
        candidate2.setPublisher(Publisher.PA);
        candidate2.setCanonicalUri("candidate2");

        List<ScoredCandidates<Item>> equivalents = ImmutableList.of(
                DefaultScoredCandidates.<Item>fromSource("A Source")
                        .addEquivalent(candidate1, Score.valueOf(score1))
                        .addEquivalent(candidate2, Score.valueOf(score2))
                        .build()
        );

        EquivalenceResult equivalenceResult = resultBuilder.resultFor(
                item,
                equivalents,
                new DefaultDescription()
        );
        return equivalenceResult;
    }

    private Item itemWithBroadcast(DateTime startTime, DateTime endTime) {
        Item item = new Item();
        Version version = new Version();
        Broadcast broadcast1 = new Broadcast("", startTime, endTime);
        version.setBroadcasts(ImmutableSet.of(broadcast1));
        item.setVersions(ImmutableSet.of(version));
        return item;
    }

    private Brand brandWithBroadcast(DateTime startTime, DateTime endTime) {
        Brand brand = new Brand();
        Version version = new Version();
        Broadcast broadcast1 = new Broadcast("", startTime, endTime);
        version.setBroadcasts(ImmutableSet.of(broadcast1));
        brand.setVersions(ImmutableSet.of(version));
        return brand;
    }

    private Series seriesWithBroadcast(DateTime startTime, DateTime endTime) {
        Series series = new Series();
        Version version = new Version();
        Broadcast broadcast1 = new Broadcast("", startTime, endTime);
        version.setBroadcasts(ImmutableSet.of(broadcast1));
        series.setVersions(ImmutableSet.of(version));
        return series;
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
    public void checkDoesNotEquivalateTwoItemsWithSameScore() {
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
    public void checkDoesntEquivalateTwoItemsWithDifferentScore() {
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
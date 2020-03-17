package org.atlasapi.equiv.update.updaters.providers.item.barb;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.atlasapi.equiv.generators.barb.BarbAliasEquivalenceGeneratorAndScorer;
import org.atlasapi.equiv.generators.barb.BarbBbcActualTransmissionItemEquivalenceGeneratorAndScorer;
import org.atlasapi.equiv.generators.barb.BarbBroadcastMatchingItemEquivalenceGeneratorAndScorer;
import org.atlasapi.equiv.results.combining.AddingEquivalenceCombiner;
import org.atlasapi.equiv.results.extractors.AllOverOrEqHighestNonEmptyThresholdExtractor;
import org.atlasapi.equiv.results.extractors.AllOverOrEqThresholdExtractor;
import org.atlasapi.equiv.results.filters.ConjunctiveFilter;
import org.atlasapi.equiv.results.filters.DummyContainerFilter;
import org.atlasapi.equiv.results.filters.ExclusionListFilter;
import org.atlasapi.equiv.results.filters.FilmYearFilter;
import org.atlasapi.equiv.results.filters.MediaTypeFilter;
import org.atlasapi.equiv.results.filters.MinimumScoreFilter;
import org.atlasapi.equiv.results.filters.SpecializationFilter;
import org.atlasapi.equiv.results.filters.UnpublishedContentFilter;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.scorers.DescriptionMatchingScorer;
import org.atlasapi.equiv.scorers.barb.BarbTitleMatchingItemScorer;
import org.atlasapi.equiv.update.ContentEquivalenceResultUpdater;
import org.atlasapi.equiv.update.EquivalenceResultUpdater;
import org.atlasapi.equiv.update.updaters.providers.EquivalenceResultUpdaterProvider;
import org.atlasapi.equiv.update.updaters.providers.EquivalenceUpdaterProviderDependencies;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.lookup.mongo.MongoLookupEntryStore;
import org.joda.time.Duration;

import java.util.Set;

public class BbcTxlogsItemUpdaterProvider implements EquivalenceResultUpdaterProvider<Item> {

    public final boolean isSubjectTxlog;

    private BbcTxlogsItemUpdaterProvider(boolean isSubjectTxlog) {
        this.isSubjectTxlog = isSubjectTxlog;
    }

    public static BbcTxlogsItemUpdaterProvider create(boolean isSubjectTxlog) {
        return new BbcTxlogsItemUpdaterProvider(isSubjectTxlog);
    }

    @Override
    public EquivalenceResultUpdater<Item> getUpdater(
            EquivalenceUpdaterProviderDependencies dependencies, Set<Publisher> targetPublishers
    ) {
        return ContentEquivalenceResultUpdater.<Item>builder()
                .withExcludedUris(dependencies.getExcludedUris())
                .withExcludedIds(dependencies.getExcludedIds())
                .withGenerators(
                        ImmutableSet.of(
                                new BarbAliasEquivalenceGeneratorAndScorer<>(
                                        ((MongoLookupEntryStore) dependencies.getLookupEntryStore()),
                                        dependencies.getContentResolver(),
                                        targetPublishers,
                                        Score.valueOf(10.0),
                                        Score.ZERO,
                                        false
                                ),
                                BarbBroadcastMatchingItemEquivalenceGeneratorAndScorer.builder()
                                        .withScheduleResolver(dependencies.getScheduleResolver())
                                        .withChannelResolver(dependencies.getChannelResolver())
                                        .withSupportedPublishers(targetPublishers)
                                        .withScheduleWindow(Duration.standardHours(1))
                                        .withBroadcastFlexibility(Duration.standardMinutes(10))
                                        .withShortBroadcastFlexibility(Duration.standardMinutes(10))
                                        .withShortBroadcastMaxDuration(Duration.standardMinutes(10))
                                        .withScoreOnMatch(Score.ONE)
                                        .withTitleMatchingScorer(
                                                BarbTitleMatchingItemScorer.builder()
                                                        .withContentResolver(dependencies.getContentResolver())
                                                        .withScoreOnMismatch(Score.nullScore())
                                                        .withScoreOnPartialMatch(Score.nullScore())
                                                        .withScoreOnPerfectMatch(Score.ONE)
                                                        .withContainerCacheDuration(60)
                                                        .build()
                                        )
                                        .build(),
                                new BarbBbcActualTransmissionItemEquivalenceGeneratorAndScorer(
                                        dependencies.getScheduleResolver(),
                                        dependencies.getChannelResolver(),
                                        targetPublishers,
                                        Duration.standardHours(1),
                                        null,
                                        Score.ONE
                                )
                        )
                )
                .withScorers(
                        ImmutableSet.of(
                                //The BarbAliasEquivalenceGeneratorAndScorer also adds a score
                                BarbTitleMatchingItemScorer.builder()
                                        .withContentResolver(dependencies.getContentResolver())
                                        .withScoreOnPerfectMatch(Score.valueOf(3.0))
                                        .withScoreOnPartialMatch(Score.valueOf(2.0))
                                        .withScoreOnMismatch(Score.ZERO)
                                        .withContainerCacheDuration(60)
                                        .build(),
                                DescriptionMatchingScorer.makeItemScorer()
                        )
                )
                .withCombiner(
                        new AddingEquivalenceCombiner<>()
                )
                .withFilter(
                        ConjunctiveFilter.valueOf(ImmutableList.of(
                                new MinimumScoreFilter<>(2),
                                new MediaTypeFilter<>(),
                                new SpecializationFilter<>(),
                                ExclusionListFilter.create(
                                        dependencies.getExcludedUris(),
                                        dependencies.getExcludedIds()
                                ),
                                new FilmYearFilter<>(),
                                new DummyContainerFilter<>(),
                                new UnpublishedContentFilter<>()
                        ))
                )

                // See TxlogsItemUpdaterProvider for reason behind 10-4 extractor on txlog->bbc equiv
                // Bbc to txlog should stay the same and equiv to all candidates since some
                // BBC txlogs are regional variants without bcids that still need to be equived to
                // even if one exists with a bcid. ENG-447
                .withExtractor(
                        isSubjectTxlog
                        ? new AllOverOrEqHighestNonEmptyThresholdExtractor<>(ImmutableSet.of(10D, 3D))
                        : AllOverOrEqThresholdExtractor.create(3)
                )
                .build();
    }
}

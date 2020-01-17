package org.atlasapi.equiv.update.updaters.providers.item.barb;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.atlasapi.equiv.generators.barb.BarbAliasEquivalenceGeneratorAndScorer;
import org.atlasapi.equiv.generators.barb.BarbBroadcastMatchingItemEquivalenceGeneratorAndScorer;
import org.atlasapi.equiv.results.combining.AddingEquivalenceCombiner;
import org.atlasapi.equiv.results.extractors.AllOverOrEqHighestNonEmptyThresholdExtractor;
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

public class TxlogsItemUpdaterProvider implements EquivalenceResultUpdaterProvider<Item> {


    private TxlogsItemUpdaterProvider() {

    }

    public static TxlogsItemUpdaterProvider create() {
        return new TxlogsItemUpdaterProvider();
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
                                        .withScoreOnMatch(Score.valueOf(3.0))
                                        .withTitleMatchingScorer(
                                                BarbTitleMatchingItemScorer.builder()
                                                        .withContentResolver(dependencies.getContentResolver())
                                                        .withScoreOnMismatch(Score.nullScore())
                                                        .withScoreOnPartialMatch(Score.nullScore())
                                                        .withScoreOnPerfectMatch(Score.ONE)
                                                        .withContainerCacheDuration(60)
                                                        .withCheckContainersForAllPublishers(true)
                                                        .build()
                                        )
                                        .build()
                        )
                )
                .withScorers(
                        ImmutableSet.of(
                                //The BarbAliasEquivalenceGeneratorAndScorer also adds a score
                                BarbTitleMatchingItemScorer.builder()
                                        .withContentResolver(dependencies.getContentResolver())
                                        .withScoreOnPerfectMatch(Score.valueOf(2.0))
                                        .withScoreOnPartialMatch(Score.ONE)
                                        .withScoreOnMismatch(Score.ZERO)
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
                .withExtractor(
                        // If we equiv on bcid (scoring 10) then we don't want to equiv on broadcast time
                        // This is due to an issue where some CMS and Txlog broadcasts have become incorrect
                        // and we had ended up with txlogs equived on bcid to one piece of CMS content but to
                        // another piece of CMS content (generally belonging to the same brand) on broadcast time.
                        // Since BARB equivalence is primarily driven by bcid equiv this should not prove problematic
                        // if we end up excluding some legitimate broadcast equiv since it will at least be equived on bcid
                        //
                        // N.B. extractors extract individually by publisher so if the highest threshold for
                        // one source is 10, we can still extract other publishers whose highest threshold was 4
                        new AllOverOrEqHighestNonEmptyThresholdExtractor<>(ImmutableSet.of(10D, 4D))
                )
                .build();
    }
}

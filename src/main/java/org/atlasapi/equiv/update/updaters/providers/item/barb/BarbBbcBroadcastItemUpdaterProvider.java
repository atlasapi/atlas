package org.atlasapi.equiv.update.updaters.providers.item.barb;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.atlasapi.equiv.generators.barb.BarbBroadcastMatchingItemEquivalenceGeneratorAndScorer;
import org.atlasapi.equiv.results.combining.AddingEquivalenceCombiner;
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
import org.joda.time.Duration;

import java.util.Set;

public class BarbBbcBroadcastItemUpdaterProvider implements EquivalenceResultUpdaterProvider<Item> {


    private BarbBbcBroadcastItemUpdaterProvider() {

    }

    public static BarbBbcBroadcastItemUpdaterProvider create() {
        return new BarbBbcBroadcastItemUpdaterProvider();
    }

    @Override
    public EquivalenceResultUpdater<Item> getUpdater(
            EquivalenceUpdaterProviderDependencies dependencies, Set<Publisher> targetPublishers
    ) {
        return ContentEquivalenceResultUpdater.<Item>builder()
                .withExcludedUris(dependencies.getExcludedUris())
                .withExcludedIds(dependencies.getExcludedIds())
                .withGenerator(
                        new BarbBroadcastMatchingItemEquivalenceGeneratorAndScorer(
                                dependencies.getScheduleResolver(),
                                dependencies.getChannelResolver(),
                                targetPublishers,
                                Duration.standardHours(3),
                                null,
                                Score.valueOf(3.0),
                                BarbTitleMatchingItemScorer.builder()
                                        .withContentResolver(dependencies.getContentResolver())
                                        .withScoreOnMismatch(Score.nullScore())
                                        .withScoreOnPartialMatch(Score.nullScore())
                                        .withScoreOnPerfectMatch(Score.ONE)
                                        .build()
                        )
                )
                .withScorers(
                        ImmutableSet.of(
                                BarbTitleMatchingItemScorer.builder()
                                        .withContentResolver(dependencies.getContentResolver())
                                        .withScoreOnPerfectMatch(Score.valueOf(2.0))
                                        .withScoreOnPartialMatch(Score.ONE)
                                        .withScoreOnMismatch(Score.ZERO)
                                        .build(),
                                DescriptionMatchingScorer.makeScorer()
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
                        AllOverOrEqThresholdExtractor.create(4)
                )
                .build();
    }
}

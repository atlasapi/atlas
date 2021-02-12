package org.atlasapi.equiv.update.updaters.providers.item.aenetworks;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.atlasapi.equiv.generators.aenetworks.AeBroadcastMatchingItemEquivalenceGeneratorAndScorer;
import org.atlasapi.equiv.results.combining.AddingEquivalenceCombiner;
import org.atlasapi.equiv.results.extractors.AllOverOrEqThresholdExtractor;
import org.atlasapi.equiv.results.filters.*;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.scorers.aenetworks.AeTitleMatchingItemScorer;
import org.atlasapi.equiv.update.ContentEquivalenceResultUpdater;
import org.atlasapi.equiv.update.EquivalenceResultUpdater;
import org.atlasapi.equiv.update.updaters.providers.EquivalenceResultUpdaterProvider;
import org.atlasapi.equiv.update.updaters.providers.EquivalenceUpdaterProviderDependencies;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.joda.time.Duration;

import java.util.Set;

public class AeItemUpdaterProvider implements EquivalenceResultUpdaterProvider<Item> {


    private AeItemUpdaterProvider() {
    }

    public static AeItemUpdaterProvider create() {
        return new AeItemUpdaterProvider();
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
                                AeBroadcastMatchingItemEquivalenceGeneratorAndScorer.builder()
                                        .withScheduleResolver(dependencies.getScheduleResolver())
                                        .withChannelResolver(dependencies.getChannelResolver())
                                        .withSupportedPublishers(targetPublishers)
                                        .withScheduleWindow(Duration.standardHours(1))
                                        .withBroadcastFlexibility(Duration.standardMinutes(15))
                                        .withShortBroadcastFlexibility(Duration.standardMinutes(15))
                                        .withShortBroadcastMaxDuration(Duration.standardMinutes(15))
                                        .withScoreOnMatch(Score.valueOf(2.0))
                                        .build()
                        )
                )
                .withScorers(
                        ImmutableSet.of(
                                AeTitleMatchingItemScorer.builder()
                                        .withContentResolver(dependencies.getContentResolver())
                                        .withScoreOnPerfectMatch(Score.valueOf(4.0))
                                        .withScoreOnPartialMatch(Score.valueOf(3.0))
                                        .withScoreOnMismatch(Score.ZERO)
                                        .withContainerCacheDuration(60)
                                        .build()
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
                        AllOverOrEqThresholdExtractor.create(5)
                )
                .build();

    }
}

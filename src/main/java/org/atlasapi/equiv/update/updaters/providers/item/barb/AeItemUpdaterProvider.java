package org.atlasapi.equiv.update.updaters.providers.item.barb;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.atlasapi.equiv.generators.barb.BarbAliasEquivalenceGeneratorAndScorer;
import org.atlasapi.equiv.generators.barb.BarbBbcActualTransmissionItemEquivalenceGeneratorAndScorer;
import org.atlasapi.equiv.generators.barb.BarbBroadcastMatchingItemEquivalenceGeneratorAndScorer;
import org.atlasapi.equiv.results.combining.AddingEquivalenceCombiner;
import org.atlasapi.equiv.results.extractors.AllOverOrEqHighestNonEmptyThresholdExtractor;
import org.atlasapi.equiv.results.extractors.AllOverOrEqThresholdExtractor;
import org.atlasapi.equiv.results.filters.*;
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
                                BarbBroadcastMatchingItemEquivalenceGeneratorAndScorer.builder()
                                        .withScheduleResolver(dependencies.getScheduleResolver())
                                        .withChannelResolver(dependencies.getChannelResolver())
                                        .withSupportedPublishers(targetPublishers)
                                        .withScheduleWindow(Duration.standardHours(1))
                                        .withBroadcastFlexibility(Duration.standardMinutes(10))
                                        .withShortBroadcastFlexibility(Duration.standardMinutes(10))
                                        .withShortBroadcastMaxDuration(Duration.standardMinutes(10))
                                        .withScoreOnMatch(Score.valueOf(2.0))
                                        .withTitleMatchingScorer(
                                                BarbTitleMatchingItemScorer.builder()
                                                        .withContentResolver(dependencies.getContentResolver())
                                                        .withScoreOnMismatch(Score.nullScore())
                                                        .withScoreOnPartialMatch(Score.nullScore())
                                                        .withScoreOnPerfectMatch(Score.ONE)
                                                        .withContainerCacheDuration(60)
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
                                        .withScoreOnPerfectMatch(Score.valueOf(4.0))
                                        .withScoreOnPartialMatch(Score.valueOf(3.0))
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
                .withExtractor(
                        AllOverOrEqThresholdExtractor.create(5)
                )
                .build();
    }
}

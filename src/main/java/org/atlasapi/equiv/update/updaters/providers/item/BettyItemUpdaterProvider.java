package org.atlasapi.equiv.update.updaters.providers.item;

import com.google.common.base.Predicates;
import org.atlasapi.equiv.generators.BroadcastMatchingItemEquivalenceGeneratorAndScorer;
import org.atlasapi.equiv.results.combining.NullScoreAwareAveragingCombiner;
import org.atlasapi.equiv.results.extractors.PercentThresholdEquivalenceExtractor;
import org.atlasapi.equiv.results.filters.AlwaysTrueFilter;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.scorers.BroadcastAliasScorer;
import org.atlasapi.equiv.update.ContentEquivalenceResultUpdater;
import org.atlasapi.equiv.update.EquivalenceResultUpdater;
import org.atlasapi.equiv.update.updaters.providers.EquivalenceResultUpdaterProvider;
import org.atlasapi.equiv.update.updaters.providers.EquivalenceUpdaterProviderDependencies;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.joda.time.Duration;

import java.util.Set;

public class BettyItemUpdaterProvider implements EquivalenceResultUpdaterProvider<Item> {

    private BettyItemUpdaterProvider() {
    }

    public static BettyItemUpdaterProvider create() {
        return new BettyItemUpdaterProvider();
    }

    @Override
    public EquivalenceResultUpdater<Item> getUpdater(
            EquivalenceUpdaterProviderDependencies dependencies,
            Set<Publisher> targetPublishers
    ) {

        return ContentEquivalenceResultUpdater.<Item>builder()
                .withExcludedUris(dependencies.getExcludedUris())
                .withExcludedIds(dependencies.getExcludedIds())
                .withGenerator(new BroadcastMatchingItemEquivalenceGeneratorAndScorer(
                        dependencies.getScheduleResolver(),
                        dependencies.getChannelResolver(),
                        targetPublishers,
                        Duration.standardMinutes(5),
                        Predicates.alwaysTrue()
                ))
                .withScorer(
                        new BroadcastAliasScorer(Score.negativeOne())
                )
                .withCombiner(
                        new NullScoreAwareAveragingCombiner<>()
                )
                .withFilter(
                        AlwaysTrueFilter.get()
                )
                .withExtractor(
                        new PercentThresholdEquivalenceExtractor<>(0.95)
                )
                .build();
    }
}

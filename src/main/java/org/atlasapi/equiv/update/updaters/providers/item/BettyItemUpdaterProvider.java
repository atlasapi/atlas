package org.atlasapi.equiv.update.updaters.providers.item;

import java.util.Set;

import org.atlasapi.equiv.generators.BroadcastMatchingItemEquivalenceGenerator;
import org.atlasapi.equiv.handlers.BroadcastingEquivalenceResultHandler;
import org.atlasapi.equiv.handlers.EpisodeFilteringEquivalenceResultHandler;
import org.atlasapi.equiv.handlers.EquivalenceSummaryWritingHandler;
import org.atlasapi.equiv.handlers.LookupWritingEquivalenceHandler;
import org.atlasapi.equiv.handlers.MessageQueueingResultHandler;
import org.atlasapi.equiv.handlers.ResultWritingEquivalenceHandler;
import org.atlasapi.equiv.results.combining.NullScoreAwareAveragingCombiner;
import org.atlasapi.equiv.results.extractors.PercentThresholdEquivalenceExtractor;
import org.atlasapi.equiv.results.filters.AlwaysTrueFilter;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.scorers.BroadcastAliasScorer;
import org.atlasapi.equiv.update.ContentEquivalenceUpdater;
import org.atlasapi.equiv.update.EquivalenceUpdater;
import org.atlasapi.equiv.update.updaters.providers.EquivalenceUpdaterProvider;
import org.atlasapi.equiv.update.updaters.providers.EquivalenceUpdaterProviderDependencies;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import org.joda.time.Duration;

public class BettyItemUpdaterProvider implements EquivalenceUpdaterProvider<Item> {

    private BettyItemUpdaterProvider() {
    }

    public static BettyItemUpdaterProvider create() {
        return new BettyItemUpdaterProvider();
    }

    @Override
    public EquivalenceUpdater<Item> getUpdater(
            EquivalenceUpdaterProviderDependencies dependencies,
            Set<Publisher> targetPublishers
    ) {

        return ContentEquivalenceUpdater.<Item>builder()
                .withExcludedUris(
                        dependencies.getExcludedUris()
                )
                .withGenerator(new BroadcastMatchingItemEquivalenceGenerator(
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
                .withHandler(
                        new BroadcastingEquivalenceResultHandler<>(ImmutableList.of(
                                EpisodeFilteringEquivalenceResultHandler.relaxed(
                                        new LookupWritingEquivalenceHandler<>(
                                                dependencies.getLookupWriter(),
                                                targetPublishers
                                        ),
                                        dependencies.getEquivSummaryStore()
                                ),
                                new ResultWritingEquivalenceHandler<>(
                                        dependencies.getEquivalenceResultStore()
                                ),
                                new EquivalenceSummaryWritingHandler<>(
                                        dependencies.getEquivSummaryStore()
                                ),
                                MessageQueueingResultHandler.create(
                                        dependencies.getMessageSender(),
                                        targetPublishers,
                                        dependencies.getLookupEntryStore()
                                )
                        ))
                )
                .build();
    }
}

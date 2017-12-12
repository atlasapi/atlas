package org.atlasapi.equiv.update.updaters.providers.item;

import java.util.Set;

import org.atlasapi.equiv.generators.BarbAliasEquivalenceGenerator;
import org.atlasapi.equiv.handlers.DelegatingEquivalenceResultHandler;
import org.atlasapi.equiv.handlers.EpisodeFilteringEquivalenceResultHandler;
import org.atlasapi.equiv.handlers.EquivalenceSummaryWritingHandler;
import org.atlasapi.equiv.handlers.LookupWritingEquivalenceHandler;
import org.atlasapi.equiv.handlers.ResultWritingEquivalenceHandler;
import org.atlasapi.equiv.messengers.QueueingEquivalenceResultMessenger;
import org.atlasapi.equiv.results.combining.NullScoreAwareAveragingCombiner;
import org.atlasapi.equiv.results.extractors.TopEquivalenceExtractor;
import org.atlasapi.equiv.results.filters.AlwaysTrueFilter;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.scorers.ContentAliasScorer;
import org.atlasapi.equiv.update.ContentEquivalenceUpdater;
import org.atlasapi.equiv.update.EquivalenceUpdater;
import org.atlasapi.equiv.update.updaters.providers.EquivalenceUpdaterProvider;
import org.atlasapi.equiv.update.updaters.providers.EquivalenceUpdaterProviderDependencies;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.lookup.mongo.MongoLookupEntryStore;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class BarbItemUpdaterProvider implements EquivalenceUpdaterProvider<Item> {


    private BarbItemUpdaterProvider() {

    }

    public static BarbItemUpdaterProvider create() {
        return new BarbItemUpdaterProvider();
    }

    @Override
    public EquivalenceUpdater<Item> getUpdater(
            EquivalenceUpdaterProviderDependencies dependencies, Set<Publisher> targetPublishers
    ) {
        return ContentEquivalenceUpdater.<Item>builder()
                .withExcludedUris(dependencies.getExcludedUris())
                .withExcludedIds(dependencies.getExcludedIds())
                .withGenerator(
                        BarbAliasEquivalenceGenerator.barbAliasResolvingGenerator(
                                ((MongoLookupEntryStore) dependencies.getLookupEntryStore()),
                                dependencies.getContentResolver()
                        )
                )
                .withScorers(
                        ImmutableSet.of(new ContentAliasScorer(Score.nullScore()))
                )
                .withCombiner(
                        new NullScoreAwareAveragingCombiner<>()
                )
                .withFilter(
                        AlwaysTrueFilter.get()
                )
                .withExtractor(
                        new TopEquivalenceExtractor<>()
                )
                .withHandler(
                        new DelegatingEquivalenceResultHandler(ImmutableList.of(
                                EpisodeFilteringEquivalenceResultHandler.relaxed(
                                        LookupWritingEquivalenceHandler.create(
                                                dependencies.getLookupWriter()
                                        ),
                                        dependencies.getEquivSummaryStore()
                                ),
                                new ResultWritingEquivalenceHandler<>(
                                        dependencies.getEquivalenceResultStore()
                                ),
                                new EquivalenceSummaryWritingHandler<>(
                                        dependencies.getEquivSummaryStore()
                                )
                        ))
                )
                .withMessenger(
                        QueueingEquivalenceResultMessenger.create(
                                dependencies.getMessageSender(),
                                dependencies.getLookupEntryStore()
                        )
                )
                .build();
    }
}

package org.atlasapi.equiv.update.updaters.providers.item;

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.atlasapi.equiv.generators.BarbAliasEquivalenceGenerator;
import org.atlasapi.equiv.generators.BarbTitleGenerator;
import org.atlasapi.equiv.generators.BroadcastMatchingItemEquivalenceGenerator;
import org.atlasapi.equiv.handlers.DelegatingEquivalenceResultHandler;
import org.atlasapi.equiv.handlers.EpisodeFilteringEquivalenceResultHandler;
import org.atlasapi.equiv.handlers.EquivalenceSummaryWritingHandler;
import org.atlasapi.equiv.handlers.LookupWritingEquivalenceHandler;
import org.atlasapi.equiv.handlers.ResultWritingEquivalenceHandler;
import org.atlasapi.equiv.messengers.QueueingEquivalenceResultMessenger;
import org.atlasapi.equiv.results.combining.AddingEquivalenceCombiner;
import org.atlasapi.equiv.results.extractors.AllOverOrEqThresholdExtractor;
import org.atlasapi.equiv.results.filters.ConjunctiveFilter;
import org.atlasapi.equiv.results.filters.DummyContainerFilter;
import org.atlasapi.equiv.results.filters.ExclusionListFilter;
import org.atlasapi.equiv.results.filters.FilmFilter;
import org.atlasapi.equiv.results.filters.MediaTypeFilter;
import org.atlasapi.equiv.results.filters.MinimumScoreFilter;
import org.atlasapi.equiv.results.filters.SpecializationFilter;
import org.atlasapi.equiv.results.filters.UnpublishedContentFilter;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.scorers.DescriptionMatchingScorer;
import org.atlasapi.equiv.scorers.TitleMatchingItemScorer;
import org.atlasapi.equiv.update.ContentEquivalenceUpdater;
import org.atlasapi.equiv.update.EquivalenceUpdater;
import org.atlasapi.equiv.update.updaters.providers.EquivalenceUpdaterProvider;
import org.atlasapi.equiv.update.updaters.providers.EquivalenceUpdaterProviderDependencies;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.lookup.mongo.MongoLookupEntryStore;
import org.joda.time.Duration;

import java.util.Set;

public class TxlogsItemUpdaterProvider implements EquivalenceUpdaterProvider<Item> {


    private TxlogsItemUpdaterProvider() {

    }

    public static TxlogsItemUpdaterProvider create() {
        return new TxlogsItemUpdaterProvider();
    }

    @Override
    public EquivalenceUpdater<Item> getUpdater(
            EquivalenceUpdaterProviderDependencies dependencies, Set<Publisher> targetPublishers
    ) {
        return ContentEquivalenceUpdater.<Item>builder()
                .withExcludedUris(dependencies.getExcludedUris())
                .withExcludedIds(dependencies.getExcludedIds())
                .withGenerators(
                        ImmutableSet.of(
                                BarbAliasEquivalenceGenerator.barbAliasResolvingGenerator(
                                        ((MongoLookupEntryStore) dependencies.getLookupEntryStore()),
                                        dependencies.getContentResolver()
                                ),
                                new BroadcastMatchingItemEquivalenceGenerator(
                                        dependencies.getScheduleResolver(),
                                        dependencies.getChannelResolver(),
                                        targetPublishers,
                                        Duration.standardMinutes(5),
                                        Predicates.alwaysTrue()
                                ),
                                BarbTitleGenerator.barbTitleGenerator(
                                        ((MongoLookupEntryStore) dependencies.getLookupEntryStore()),
                                        dependencies.getContentResolver(),
                                        ImmutableSet.of(BarbTitleGenerator.BBC_BROADCAST_GROUP),
                                        Score.valueOf(5.0)
                                )
                        )
                )
                .withScorers(
                        ImmutableSet.of(
                                //The BarbAliasEquivalenceGenerator also adds a score
                                new TitleMatchingItemScorer(Score.nullScore()),
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
                                new FilmFilter<>(),
                                new DummyContainerFilter<>(),
                                new UnpublishedContentFilter<>()
                        ))
                )
                .withExtractor(
                        AllOverOrEqThresholdExtractor.create(3)
                )
                .withHandler(
                        new DelegatingEquivalenceResultHandler<>(ImmutableList.of(
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

package org.atlasapi.equiv.update.updaters.providers.item;

import java.util.Set;

import org.atlasapi.application.v3.DefaultApplication;
import org.atlasapi.equiv.generators.ContainerCandidatesItemEquivalenceGenerator;
import org.atlasapi.equiv.generators.FilmEquivalenceGenerator;
import org.atlasapi.equiv.handlers.DelegatingEquivalenceResultHandler;
import org.atlasapi.equiv.handlers.EpisodeFilteringEquivalenceResultHandler;
import org.atlasapi.equiv.handlers.EquivalenceSummaryWritingHandler;
import org.atlasapi.equiv.handlers.LookupWritingEquivalenceHandler;
import org.atlasapi.equiv.handlers.ResultWritingEquivalenceHandler;
import org.atlasapi.equiv.messengers.QueueingEquivalenceResultMessenger;
import org.atlasapi.equiv.results.combining.NullScoreAwareAveragingCombiner;
import org.atlasapi.equiv.results.combining.RequiredScoreFilteringCombiner;
import org.atlasapi.equiv.results.extractors.PercentThresholdAboveNextBestMatchEquivalenceExtractor;
import org.atlasapi.equiv.results.filters.ConjunctiveFilter;
import org.atlasapi.equiv.results.filters.DummyContainerFilter;
import org.atlasapi.equiv.results.filters.ExclusionListFilter;
import org.atlasapi.equiv.results.filters.FilmFilter;
import org.atlasapi.equiv.results.filters.MediaTypeFilter;
import org.atlasapi.equiv.results.filters.MinimumScoreFilter;
import org.atlasapi.equiv.results.filters.PublisherFilter;
import org.atlasapi.equiv.results.filters.SpecializationFilter;
import org.atlasapi.equiv.results.filters.UnpublishedContentFilter;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.scorers.SequenceItemScorer;
import org.atlasapi.equiv.scorers.SeriesSequenceItemScorer;
import org.atlasapi.equiv.scorers.TitleMatchingItemScorer;
import org.atlasapi.equiv.update.ContentEquivalenceUpdater;
import org.atlasapi.equiv.update.EquivalenceUpdater;
import org.atlasapi.equiv.update.updaters.providers.EquivalenceUpdaterProvider;
import org.atlasapi.equiv.update.updaters.providers.EquivalenceUpdaterProviderDependencies;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class BtVodItemUpdaterProvider implements EquivalenceUpdaterProvider<Item> {

    private BtVodItemUpdaterProvider() {
    }

    public static BtVodItemUpdaterProvider create() {
        return new BtVodItemUpdaterProvider();
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
                .withGenerators(
                        ImmutableSet.of(
                                new ContainerCandidatesItemEquivalenceGenerator(
                                        dependencies.getContentResolver(),
                                        dependencies.getEquivSummaryStore()
                                ),
                                new FilmEquivalenceGenerator(
                                        dependencies.getSearchResolver(),
                                        targetPublishers,
                                        DefaultApplication.createWithReads(
                                                ImmutableList.copyOf(targetPublishers)
                                        ),
                                        true
                                )
                        )
                )
                .withScorers(
                        ImmutableSet.of(
                                new TitleMatchingItemScorer(),
                                // Hierarchies are known to be inconsistent between the BT VoD
                                // catalogue and others, so we want to ascribe less weight
                                // to sequence scoring
                                new SequenceItemScorer(Score.valueOf(0.5)),
                                new SeriesSequenceItemScorer()
                        )
                )
                .withCombiner(
                        new RequiredScoreFilteringCombiner<>(
                                new NullScoreAwareAveragingCombiner<>(),
                                ImmutableSet.of(
                                        TitleMatchingItemScorer.NAME,
                                        SequenceItemScorer.SEQUENCE_SCORER
                                )
                        )
                )
                .withFilter(
                        ConjunctiveFilter.valueOf(ImmutableList.of(
                                new MinimumScoreFilter<>(0.25),
                                new MediaTypeFilter<>(),
                                new SpecializationFilter<>(),
                                new PublisherFilter<>(),
                                new ExclusionListFilter<>(dependencies.getExcludedUris()),
                                new FilmFilter<>(),
                                new DummyContainerFilter<>(),
                                new UnpublishedContentFilter<>()
                        ))
                )
                .withExtractor(
                        PercentThresholdAboveNextBestMatchEquivalenceExtractor
                                .atLeastNTimesGreater(1.5)
                )
                .withHandler(
                        new DelegatingEquivalenceResultHandler<>(ImmutableList.of(
                                EpisodeFilteringEquivalenceResultHandler.strict(
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

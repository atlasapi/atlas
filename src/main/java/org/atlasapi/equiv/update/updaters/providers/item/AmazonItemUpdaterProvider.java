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
import org.atlasapi.equiv.results.combining.AddingEquivalenceCombiner;
import org.atlasapi.equiv.results.combining.RequiredScoreFilteringCombiner;
import org.atlasapi.equiv.results.extractors.AllWithTheSameHighScoreExtractor;
import org.atlasapi.equiv.results.extractors.PercentThresholdEquivalenceExtractor;
import org.atlasapi.equiv.results.extractors.RemoveAndCombineExtractor;
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
import org.atlasapi.equiv.scorers.SequenceItemScorer;
import org.atlasapi.equiv.scorers.TitleMatchingItemScorer;
import org.atlasapi.equiv.update.ContentEquivalenceUpdater;
import org.atlasapi.equiv.update.EquivalenceUpdater;
import org.atlasapi.equiv.update.updaters.providers.EquivalenceUpdaterProvider;
import org.atlasapi.equiv.update.updaters.providers.EquivalenceUpdaterProviderDependencies;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class AmazonItemUpdaterProvider implements EquivalenceUpdaterProvider<Item> {

    private AmazonItemUpdaterProvider() {
    }

    public static AmazonItemUpdaterProvider create() {
        return new AmazonItemUpdaterProvider();
    }

    @Override
    public EquivalenceUpdater<Item> getUpdater(
            EquivalenceUpdaterProviderDependencies dependencies,
            Set<Publisher> targetPublishers
    ) {
        return ContentEquivalenceUpdater.<Item>builder()
                .withExcludedUris(dependencies.getExcludedUris())
                .withExcludedIds(dependencies.getExcludedIds())
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
                                        true)
                        )
                )
                .withScorers(
                        ImmutableSet.of(
                                new TitleMatchingItemScorer(),
                                DescriptionMatchingScorer.makeScorer(),
                                new SequenceItemScorer(Score.ONE)
                        )
                )
                .withCombiner(
                        new RequiredScoreFilteringCombiner<>(
                                new AddingEquivalenceCombiner<>(),
                                TitleMatchingItemScorer.NAME
                        )
                )
                .withFilter(
                        ConjunctiveFilter.valueOf(ImmutableList.of(
                                new MinimumScoreFilter<>(0.25),
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
                .withExtractors(
                        ImmutableList.of(
                                //get all items that tie at the top of the scores with a score of at least 3.
                                //this should equiv all amazon versions of the same content together
                                //then let it equate with other stuff as well.
                                RemoveAndCombineExtractor.create(
                                        AllWithTheSameHighScoreExtractor.create(3.1),
                                        PercentThresholdEquivalenceExtractor.moreThanPercent(90)

                                )

                        )
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

package org.atlasapi.equiv.update.updaters.providers.container;

import java.util.Set;

import org.atlasapi.equiv.generators.ContainerChildEquivalenceGenerator;
import org.atlasapi.equiv.generators.ScalingEquivalenceGenerator;
import org.atlasapi.equiv.generators.TitleSearchGenerator;
import org.atlasapi.equiv.handlers.DelegatingEquivalenceResultHandler;
import org.atlasapi.equiv.handlers.EpisodeMatchingEquivalenceHandler;
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
import org.atlasapi.equiv.scorers.EquivalenceScorer;
import org.atlasapi.equiv.scorers.TitleMatchingContainerScorer;
import org.atlasapi.equiv.update.ContentEquivalenceUpdater;
import org.atlasapi.equiv.update.EquivalenceUpdater;
import org.atlasapi.equiv.update.updaters.providers.EquivalenceUpdaterProvider;
import org.atlasapi.equiv.update.updaters.providers.EquivalenceUpdaterProviderDependencies;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Publisher;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class StandardTopLevelContainerUpdaterProvider
        implements EquivalenceUpdaterProvider<Container> {

    private StandardTopLevelContainerUpdaterProvider() { }

    public static StandardTopLevelContainerUpdaterProvider create() {
        return new StandardTopLevelContainerUpdaterProvider();
    }

    @Override
    public EquivalenceUpdater<Container> getUpdater(
            EquivalenceUpdaterProviderDependencies dependencies,
            Set<Publisher> targetPublishers
    ) {
        return ContentEquivalenceUpdater.<Container> builder()
                .withExcludedUris(dependencies.getExcludedUris())
                .withExcludedIds(dependencies.getExcludedIds())
                .withGenerators(
                        ImmutableSet.of(
                                TitleSearchGenerator.create(
                                        dependencies.getSearchResolver(),
                                        Container.class,
                                        targetPublishers,
                                        2
                                ),
                                ScalingEquivalenceGenerator.scale(
                                        new ContainerChildEquivalenceGenerator(
                                                dependencies.getContentResolver(),
                                                dependencies.getEquivSummaryStore()
                                        ),
                                        20
                                )
                        )
                )
                .withScorers(
                        ImmutableSet.<EquivalenceScorer<Container>> of(
                                new TitleMatchingContainerScorer((double) 2)
                        )
                )
                .withCombiner(
                        new RequiredScoreFilteringCombiner<>(
                                new NullScoreAwareAveragingCombiner<>(),
                                TitleMatchingContainerScorer.NAME
                        )
                )
                .withFilter(
                        ConjunctiveFilter.valueOf(ImmutableList.of(
                                new MinimumScoreFilter<>(0.25),
                                new MediaTypeFilter<>(),
                                new SpecializationFilter<>(),
                                new PublisherFilter<>(),
                                new ExclusionListFilter<>(
                                        dependencies.getExcludedUris(),
                                        dependencies.getExcludedIds()
                                ),
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
                                LookupWritingEquivalenceHandler.create(
                                        dependencies.getLookupWriter()
                                ),
                                new EpisodeMatchingEquivalenceHandler(
                                        dependencies.getContentResolver(),
                                        dependencies.getEquivSummaryStore(),
                                        dependencies.getLookupWriter(),
                                        targetPublishers
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

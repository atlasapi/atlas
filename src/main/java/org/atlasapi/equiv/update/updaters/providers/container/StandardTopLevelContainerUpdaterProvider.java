package org.atlasapi.equiv.update.updaters.providers.container;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.atlasapi.equiv.generators.ContainerChildEquivalenceGenerator;
import org.atlasapi.equiv.generators.ScalingEquivalenceGenerator;
import org.atlasapi.equiv.generators.TitleSearchGenerator;
import org.atlasapi.equiv.results.combining.NullScoreAwareAveragingCombiner;
import org.atlasapi.equiv.results.combining.RequiredScoreFilteringCombiner;
import org.atlasapi.equiv.results.extractors.MultipleCandidateExtractor;
import org.atlasapi.equiv.results.extractors.PercentThresholdAboveNextBestMatchEquivalenceExtractor;
import org.atlasapi.equiv.results.filters.ConjunctiveFilter;
import org.atlasapi.equiv.results.filters.DummyContainerFilter;
import org.atlasapi.equiv.results.filters.ExclusionListFilter;
import org.atlasapi.equiv.results.filters.FilmYearFilter;
import org.atlasapi.equiv.results.filters.MediaTypeFilter;
import org.atlasapi.equiv.results.filters.MinimumScoreFilter;
import org.atlasapi.equiv.results.filters.PublisherFilter;
import org.atlasapi.equiv.results.filters.SpecializationFilter;
import org.atlasapi.equiv.results.filters.UnpublishedContentFilter;
import org.atlasapi.equiv.scorers.EquivalenceScorer;
import org.atlasapi.equiv.scorers.TitleMatchingContainerScorer;
import org.atlasapi.equiv.update.ContentEquivalenceResultUpdater;
import org.atlasapi.equiv.update.EquivalenceResultUpdater;
import org.atlasapi.equiv.update.updaters.providers.EquivalenceResultUpdaterProvider;
import org.atlasapi.equiv.update.updaters.providers.EquivalenceUpdaterProviderDependencies;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Publisher;

import java.util.Set;

public class StandardTopLevelContainerUpdaterProvider
        implements EquivalenceResultUpdaterProvider<Container> {

    private StandardTopLevelContainerUpdaterProvider() { }

    public static StandardTopLevelContainerUpdaterProvider create() {
        return new StandardTopLevelContainerUpdaterProvider();
    }

    @Override
    public EquivalenceResultUpdater<Container> getUpdater(
            EquivalenceUpdaterProviderDependencies dependencies,
            Set<Publisher> targetPublishers
    ) {
        return ContentEquivalenceResultUpdater.<Container> builder()
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
                                ExclusionListFilter.create(
                                        dependencies.getExcludedUris(),
                                        dependencies.getExcludedIds()
                                ),
                                new FilmYearFilter<>(),
                                new DummyContainerFilter<>(),
                                new UnpublishedContentFilter<>()
                        ))
                )
                .withExtractors(
                        ImmutableList.of(
                                MultipleCandidateExtractor.create(),
                                PercentThresholdAboveNextBestMatchEquivalenceExtractor
                                        .atLeastNTimesGreater(1.5)
                        )
                )
                .build();
    }
}

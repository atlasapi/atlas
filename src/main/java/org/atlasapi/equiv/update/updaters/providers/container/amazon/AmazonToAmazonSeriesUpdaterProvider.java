package org.atlasapi.equiv.update.updaters.providers.container.amazon;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.atlasapi.equiv.generators.ContainerCandidatesContainerEquivalenceGenerator;
import org.atlasapi.equiv.results.combining.AddingEquivalenceCombiner;
import org.atlasapi.equiv.results.extractors.AllWithTheSameHighscoreAndPublisherExtractor;
import org.atlasapi.equiv.results.filters.ConjunctiveFilter;
import org.atlasapi.equiv.results.filters.ContainerHierarchyFilter;
import org.atlasapi.equiv.results.filters.ExclusionListFilter;
import org.atlasapi.equiv.results.filters.FilmYearFilter;
import org.atlasapi.equiv.results.filters.MediaTypeFilter;
import org.atlasapi.equiv.results.filters.MinimumScoreFilter;
import org.atlasapi.equiv.results.filters.SpecializationFilter;
import org.atlasapi.equiv.results.filters.UnpublishedContentFilter;
import org.atlasapi.equiv.scorers.SequenceContainerScorer;
import org.atlasapi.equiv.scorers.TitleMatchingContainerScorer;
import org.atlasapi.equiv.update.ContentEquivalenceResultUpdater;
import org.atlasapi.equiv.update.EquivalenceResultUpdater;
import org.atlasapi.equiv.update.updaters.providers.EquivalenceResultUpdaterProvider;
import org.atlasapi.equiv.update.updaters.providers.EquivalenceUpdaterProviderDependencies;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Publisher;

import java.util.Set;

/***
 * Like {@link AmazonSeriesUpdaterProvider}, except different extractor (as before the split of
 * Amazon equiv into Amazon-Amazon and Amazon-Everything else)
 */
public class AmazonToAmazonSeriesUpdaterProvider implements EquivalenceResultUpdaterProvider<Container> {

    private AmazonToAmazonSeriesUpdaterProvider() {
    }

    public static AmazonToAmazonSeriesUpdaterProvider create() {
        return new AmazonToAmazonSeriesUpdaterProvider();
    }

    @Override
    public EquivalenceResultUpdater<Container> getUpdater(
            EquivalenceUpdaterProviderDependencies dependencies,
            Set<Publisher> targetPublishers
    ) {
        return ContentEquivalenceResultUpdater.<Container>builder()
                .withExcludedUris(dependencies.getExcludedUris())
                .withExcludedIds(dependencies.getExcludedIds())
                .withGenerators(
                        ImmutableSet.of(
                                // This should only generate the candidates based on the container's
                                // strong candidates, otherwise we get candidates from other brands
                                new ContainerCandidatesContainerEquivalenceGenerator(
                                        dependencies.getContentResolver(),
                                        dependencies.getEquivSummaryStore(),
                                        targetPublishers,
                                        true,
                                        true)
                        )
                )
                .withScorers(
                        ImmutableSet.of(
                                new TitleMatchingContainerScorer(2),
                                new SequenceContainerScorer()
                        )
                )
                .withCombiner(
                        new AddingEquivalenceCombiner<>()
                )
                .withFilter(
                        ConjunctiveFilter.valueOf(ImmutableList.of(
                                new MinimumScoreFilter<>(0.9999),
                                new MediaTypeFilter<>(),
                                new SpecializationFilter<>(),
                                ExclusionListFilter.create(
                                        dependencies.getExcludedUris(),
                                        dependencies.getExcludedIds()
                                ),
                                new FilmYearFilter<>(),
                                new UnpublishedContentFilter<>(),
                                new ContainerHierarchyFilter()
                        ))
                )
                .withExtractor(
                        //get all items that scored perfectly everywhere.
                        //this should equiv all amazon versions of the same content together
                        AllWithTheSameHighscoreAndPublisherExtractor.create(3.00)
                )
                .build();
    }
}

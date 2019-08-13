package org.atlasapi.equiv.update.updaters.providers.container.amazon;

import java.util.Set;

import org.atlasapi.equiv.generators.amazon.AmazonTitleGenerator;
import org.atlasapi.equiv.results.combining.AddingEquivalenceCombiner;
import org.atlasapi.equiv.results.combining.RequiredScoreFilteringCombiner;
import org.atlasapi.equiv.results.extractors.AllWithTheSameHighscoreAndPublisherExtractor;
import org.atlasapi.equiv.results.filters.ConjunctiveFilter;
import org.atlasapi.equiv.results.filters.DummyContainerFilter;
import org.atlasapi.equiv.results.filters.ExclusionListFilter;
import org.atlasapi.equiv.results.filters.MediaTypeFilter;
import org.atlasapi.equiv.results.filters.MinimumScoreFilter;
import org.atlasapi.equiv.results.filters.SpecializationFilter;
import org.atlasapi.equiv.results.filters.UnpublishedContentFilter;
import org.atlasapi.equiv.results.scores.ScoreThreshold;
import org.atlasapi.equiv.scorers.TitleMatchingContainerScorer;
import org.atlasapi.equiv.update.ContentEquivalenceResultUpdater;
import org.atlasapi.equiv.update.EquivalenceResultUpdater;
import org.atlasapi.equiv.update.updaters.providers.EquivalenceResultUpdaterProvider;
import org.atlasapi.equiv.update.updaters.providers.EquivalenceUpdaterProviderDependencies;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Publisher;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class AmazonToAmazonContainerUpdaterProvider implements EquivalenceResultUpdaterProvider<Container> {

    private AmazonToAmazonContainerUpdaterProvider() {
    }

    public static AmazonToAmazonContainerUpdaterProvider create() {
        return new AmazonToAmazonContainerUpdaterProvider();
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
                        //whatever generators are used here, should prevent the creation of
                        //candidates which are the item itself (because there is no further filtering
                        //to remove them, whereas the Publisher filter used elsewhere does that).
                        ImmutableSet.of(
                                new AmazonTitleGenerator<>(
                                        dependencies.getAmazonTitleIndexStore(),
                                        dependencies.getContentResolver(),
                                        Container.class,
                                        Publisher.AMAZON_UNBOX
                                )
                        )
                )
                .withScorers(
                        ImmutableSet.of(
                                new TitleMatchingContainerScorer(2)
                        )
                )
                .withCombiner(
                        new RequiredScoreFilteringCombiner<>(
                                new AddingEquivalenceCombiner<>(),
                                TitleMatchingContainerScorer.NAME,
                                ScoreThreshold.greaterThanOrEqual(2)
                        )
                )
                .withFilter(
                        ConjunctiveFilter.valueOf(ImmutableList.of(
                                new MinimumScoreFilter<>(1.99999),
                                new MediaTypeFilter<>(),
                                new SpecializationFilter<>(),
                                ExclusionListFilter.create(
                                        dependencies.getExcludedUris(),
                                        dependencies.getExcludedIds()
                                ),
                                new DummyContainerFilter<>(),
                                new UnpublishedContentFilter<>()
                        ))
                )
                .withExtractor(
                        //Get all amazon items with same score that scored at least perfect for title
                        AllWithTheSameHighscoreAndPublisherExtractor.create(2.00)
                )
                .build();
    }
}

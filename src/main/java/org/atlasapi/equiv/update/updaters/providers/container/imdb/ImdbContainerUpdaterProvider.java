package org.atlasapi.equiv.update.updaters.providers.container.imdb;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.atlasapi.equiv.generators.AliasResolvingEquivalenceGenerator;
import org.atlasapi.equiv.generators.ContainerChildEquivalenceGenerator;
import org.atlasapi.equiv.generators.TitleSearchGenerator;
import org.atlasapi.equiv.results.combining.AddingEquivalenceCombiner;
import org.atlasapi.equiv.results.extractors.AllOverOrEqThresholdExtractor;
import org.atlasapi.equiv.results.filters.ConjunctiveFilter;
import org.atlasapi.equiv.results.filters.ContainerHierarchyFilter;
import org.atlasapi.equiv.results.filters.DummyContainerFilter;
import org.atlasapi.equiv.results.filters.ExclusionListFilter;
import org.atlasapi.equiv.results.filters.MediaTypeFilter;
import org.atlasapi.equiv.results.filters.MinimumScoreFilter;
import org.atlasapi.equiv.results.filters.UnpublishedContentFilter;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.scorers.ContainerYearScorer;
import org.atlasapi.equiv.scorers.DescriptionMatchingScorer;
import org.atlasapi.equiv.scorers.DescriptionTitleMatchingScorer;
import org.atlasapi.equiv.scorers.TitleMatchingContainerScorer;
import org.atlasapi.equiv.update.ContentEquivalenceResultUpdater;
import org.atlasapi.equiv.update.EquivalenceResultUpdater;
import org.atlasapi.equiv.update.updaters.providers.EquivalenceResultUpdaterProvider;
import org.atlasapi.equiv.update.updaters.providers.EquivalenceUpdaterProviderDependencies;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Publisher;

import java.util.Set;

public class ImdbContainerUpdaterProvider implements EquivalenceResultUpdaterProvider<Container> {

    private final String IMDB_NAMESPACE = "imdb:id";
    private final String OLD_IMDB_NAMESPACE = "gb:imdb:resourceId";
    private final String AMAZON_IMDB_NAMESPACE = "zz:imdb:id";
    private final String JUSTWATCH_IMDB_NAMESPACE = "justwatch:imdb:id";
    private final Set<Set<String>> NAMESPACES_SET = ImmutableSet.of(
            ImmutableSet.of(IMDB_NAMESPACE, OLD_IMDB_NAMESPACE, AMAZON_IMDB_NAMESPACE, JUSTWATCH_IMDB_NAMESPACE)
    );

    private ImdbContainerUpdaterProvider() {

    }

    public static ImdbContainerUpdaterProvider create() {
        return new ImdbContainerUpdaterProvider();
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
                                AliasResolvingEquivalenceGenerator.<Container>builder()
                                        .withResolver(dependencies.getContentResolver())
                                        .withPublishers(targetPublishers)
                                        .withLookupEntryStore(dependencies.getLookupEntryStore())
                                        .withNamespacesSet(NAMESPACES_SET)
                                        .withAliasMatchingScore(Score.valueOf(3D))
                                        .withIncludeUnpublishedContent(false)
                                        .withClass(Container.class)
                                        .build(),
                                TitleSearchGenerator.create(
                                        dependencies.getSearchResolver(),
                                        Container.class,
                                        targetPublishers,
                                        Score.nullScore(),
                                        Score.nullScore(),
                                        true,
                                        true,
                                        true
                                ),
                                new ContainerChildEquivalenceGenerator(
                                        dependencies.getContentResolver(),
                                        dependencies.getEquivSummaryStore()
                                )
                        )
                )
                .withScorers(
                        ImmutableSet.of(
                                new TitleMatchingContainerScorer(2.0),
                                new ContainerYearScorer(Score.ONE, Score.negativeOne(), Score.nullScore()),
                                DescriptionTitleMatchingScorer.createContainerScorer(),
                                DescriptionMatchingScorer.makeContainerScorer()
                        )
                )
                .withCombiner(
                        new AddingEquivalenceCombiner<>()
                )
                .withFilter(
                        ConjunctiveFilter.valueOf(ImmutableList.of(
                                new MinimumScoreFilter<>(2.5),
                                new MediaTypeFilter<>(),
                                new DummyContainerFilter<>(),
                                new UnpublishedContentFilter<>(),
                                ExclusionListFilter.create(
                                        dependencies.getExcludedUris(),
                                        dependencies.getExcludedIds()
                                ),
                                //Amazon series have IMDb brand ids as alias, causing bad equiv;
                                //this filter will prevent that from occuring
                                new ContainerHierarchyFilter()
                        ))
                )
                .withExtractor(
                        AllOverOrEqThresholdExtractor.create(2.6)
                )
                .build();
    }
}

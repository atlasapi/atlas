package org.atlasapi.equiv.update.updaters.providers.container.imdb;

import java.util.Set;

import org.atlasapi.equiv.generators.AliasResolvingEquivalenceGenerator;
import org.atlasapi.equiv.results.combining.AddingEquivalenceCombiner;
import org.atlasapi.equiv.results.extractors.AllOverOrEqThresholdExtractor;
import org.atlasapi.equiv.results.filters.ConjunctiveFilter;
import org.atlasapi.equiv.results.filters.DummyContainerFilter;
import org.atlasapi.equiv.results.filters.ExclusionListFilter;
import org.atlasapi.equiv.results.filters.MediaTypeFilter;
import org.atlasapi.equiv.results.filters.MinimumScoreFilter;
import org.atlasapi.equiv.results.filters.UnpublishedContentFilter;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.scorers.ContainerYearScorer;
import org.atlasapi.equiv.scorers.TitleMatchingContainerScorer;
import org.atlasapi.equiv.update.ContentEquivalenceResultUpdater;
import org.atlasapi.equiv.update.EquivalenceResultUpdater;
import org.atlasapi.equiv.update.updaters.providers.EquivalenceResultUpdaterProvider;
import org.atlasapi.equiv.update.updaters.providers.EquivalenceUpdaterProviderDependencies;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Publisher;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

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
                                        .build()
                        )
                )
                .withScorers(
                        ImmutableSet.of(
                                new TitleMatchingContainerScorer(2.0),
                                new ContainerYearScorer(Score.ONE)
                        )
                )
                .withCombiner(
                        new AddingEquivalenceCombiner<>()
                )
                .withFilter(
                        ConjunctiveFilter.valueOf(ImmutableList.of(
                                new MinimumScoreFilter<>(2.9),
                                new MediaTypeFilter<>(),
                                new DummyContainerFilter<>(),
                                new UnpublishedContentFilter<>(),
                                ExclusionListFilter.create(
                                        dependencies.getExcludedUris(),
                                        dependencies.getExcludedIds()
                                )
                                //TODO add a filter that checks that the candidate is the same type
                        ))
                )
                .withExtractor(
                        AllOverOrEqThresholdExtractor.create(3)
                )
                .build();
    }
}

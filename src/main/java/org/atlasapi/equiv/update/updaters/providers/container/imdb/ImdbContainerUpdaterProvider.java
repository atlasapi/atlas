package org.atlasapi.equiv.update.updaters.providers.container.imdb;

import java.util.Set;

import javax.annotation.Nullable;

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
import org.atlasapi.equiv.scorers.SoleCandidateTitleMatchingScorer;
import org.atlasapi.equiv.scorers.TitleMatchingContainerScorer;
import org.atlasapi.equiv.update.ContentEquivalenceResultUpdater;
import org.atlasapi.equiv.update.EquivalenceResultUpdater;
import org.atlasapi.equiv.update.updaters.providers.EquivalenceResultUpdaterProvider;
import org.atlasapi.equiv.update.updaters.providers.EquivalenceUpdaterProviderDependencies;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Publisher;

import com.metabroadcast.common.stream.MoreCollectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class ImdbContainerUpdaterProvider implements EquivalenceResultUpdaterProvider<Container> {

    private final Set<Set<String>> namespacesSet;

    private ImdbContainerUpdaterProvider(@Nullable Set<Set<String>> namespacesSet) {
        this.namespacesSet = namespacesSet == null
                             ? ImmutableSet.of()
                             : namespacesSet.stream()
                                     .map(ImmutableSet::copyOf)
                                     .collect(MoreCollectors.toImmutableSet());
    }

    public static ImdbContainerUpdaterProvider create(@Nullable Set<Set<String>> namespacesSet) {
        return new ImdbContainerUpdaterProvider(namespacesSet);
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
                                        .withNamespacesSet(namespacesSet)
                                        .withAliasMatchingScore(Score.valueOf(3D))
                                        .withIncludeUnpublishedContent(false)
                                        .withClass(Container.class)
                                        .build(),
                                TitleSearchGenerator.create(
                                        dependencies.getOwlSearchResolver(),
                                        Container.class,
                                        targetPublishers,
                                        Score.nullScore(),
                                        Score.nullScore(),
                                        true,
                                        true,
                                        true
                                ),
                                TitleSearchGenerator.create(
                                        dependencies.getDeerSearchResolver(),
                                        Container.class,
                                        targetPublishers,
                                        Score.ZERO,
                                        Score.ZERO,
                                        true,
                                        true,
                                        true
                                ),
                                new ContainerChildEquivalenceGenerator(
                                        dependencies.getContentResolver(),
                                        dependencies.getEquivSummaryStore(),
                                        targetPublishers
                                )
                        )
                )
                .withScorers(
                        ImmutableSet.of(
                                new TitleMatchingContainerScorer(
                                        Score.valueOf(2D),
                                        Score.ONE,
                                        true
                                ),
                                new SoleCandidateTitleMatchingScorer<>(
                                        dependencies.getOwlSearchResolver(),
                                        Score.ONE,
                                        Score.nullScore(),
                                        Container.class
                                ),
                                new ContainerYearScorer(
                                        Score.ONE,
                                        Score.negativeOne(),
                                        Score.nullScore()
                                ),
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

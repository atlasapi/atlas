package org.atlasapi.equiv.update.updaters.providers.container;

import java.util.Set;

import javax.annotation.Nullable;

import org.atlasapi.equiv.generators.AliasResolvingEquivalenceGenerator;
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
import org.atlasapi.equiv.update.ContentEquivalenceResultUpdater;
import org.atlasapi.equiv.update.EquivalenceResultUpdater;
import org.atlasapi.equiv.update.updaters.providers.EquivalenceResultUpdaterProvider;
import org.atlasapi.equiv.update.updaters.providers.EquivalenceUpdaterProviderDependencies;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Publisher;

import com.metabroadcast.common.stream.MoreCollectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class AliasContainerUpdaterProvider implements EquivalenceResultUpdaterProvider<Container> {

    private final Set<Set<String>> namespacesSet;

    private AliasContainerUpdaterProvider(@Nullable Set<Set<String>> namespacesSet) {
        this.namespacesSet = namespacesSet == null
                             ? ImmutableSet.of()
                             : namespacesSet.stream()
                                     .map(ImmutableSet::copyOf)
                                     .collect(MoreCollectors.toImmutableSet());
    }

    public static AliasContainerUpdaterProvider create(@Nullable Set<Set<String>> namespacesSet) {
        return new AliasContainerUpdaterProvider(namespacesSet);
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
                                        .build()
                        )
                )
                .withScorers(
                        ImmutableSet.of()
                )
                .withCombiner(
                        new AddingEquivalenceCombiner<>()
                )
                .withFilter(
                        ConjunctiveFilter.valueOf(ImmutableList.of(
                                new MinimumScoreFilter<>(2.9),
                                new MediaTypeFilter<>(),
                                new DummyContainerFilter<>(),
                                new ContainerHierarchyFilter(),
                                new UnpublishedContentFilter<>(),
                                ExclusionListFilter.create(
                                        dependencies.getExcludedUris(),
                                        dependencies.getExcludedIds()
                                )
                        ))
                )
                .withExtractor(
                        AllOverOrEqThresholdExtractor.create(3)
                )
                .build();
    }

}

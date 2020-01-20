package org.atlasapi.equiv.update.updaters.providers.item;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.stream.MoreCollectors;
import org.atlasapi.equiv.generators.AliasResolvingEquivalenceGenerator;
import org.atlasapi.equiv.results.combining.AddingEquivalenceCombiner;
import org.atlasapi.equiv.results.extractors.AllOverOrEqThresholdExtractor;
import org.atlasapi.equiv.results.filters.ConjunctiveFilter;
import org.atlasapi.equiv.results.filters.DummyContainerFilter;
import org.atlasapi.equiv.results.filters.ExclusionListFilter;
import org.atlasapi.equiv.results.filters.FilmAndEpisodeFilter;
import org.atlasapi.equiv.results.filters.MediaTypeFilter;
import org.atlasapi.equiv.results.filters.MinimumScoreFilter;
import org.atlasapi.equiv.results.filters.UnpublishedContentFilter;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.update.ContentEquivalenceResultUpdater;
import org.atlasapi.equiv.update.EquivalenceResultUpdater;
import org.atlasapi.equiv.update.updaters.providers.EquivalenceResultUpdaterProvider;
import org.atlasapi.equiv.update.updaters.providers.EquivalenceUpdaterProviderDependencies;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;

import javax.annotation.Nullable;
import java.util.Set;

public class AliasItemUpdaterProvider implements EquivalenceResultUpdaterProvider<Item> {

    private final Set<Set<String>> namespacesSet;

    private AliasItemUpdaterProvider(@Nullable Set<Set<String>> namespacesSet) {
        this.namespacesSet = namespacesSet == null
                ? ImmutableSet.of()
                : namespacesSet.stream()
                .map(ImmutableSet::copyOf)
                .collect(MoreCollectors.toImmutableSet());
    }

    public static AliasItemUpdaterProvider create(@Nullable Set<Set<String>> namespacesSet) {
        return new AliasItemUpdaterProvider(namespacesSet);
    }

    @Override
    public EquivalenceResultUpdater<Item> getUpdater(
            EquivalenceUpdaterProviderDependencies dependencies,
            Set<Publisher> targetPublishers
    ) {
        return ContentEquivalenceResultUpdater.<Item>builder()
                .withExcludedUris(dependencies.getExcludedUris())
                .withExcludedIds(dependencies.getExcludedIds())
                .withGenerators(
                        ImmutableSet.of(
                                AliasResolvingEquivalenceGenerator.<Item>builder()
                                    .withResolver(dependencies.getContentResolver())
                                    .withPublishers(targetPublishers)
                                    .withLookupEntryStore(dependencies.getLookupEntryStore())
                                    .withNamespacesSet(namespacesSet)
                                    .withAliasMatchingScore(Score.valueOf(3D))
                                    .withIncludeUnpublishedContent(false)
                                    .withClass(Item.class)
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
                                new FilmAndEpisodeFilter<>(),
                                new DummyContainerFilter<>(),
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

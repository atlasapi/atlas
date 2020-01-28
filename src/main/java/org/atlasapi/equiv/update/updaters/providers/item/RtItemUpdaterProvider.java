package org.atlasapi.equiv.update.updaters.providers.item;

import java.util.Set;

import javax.annotation.Nullable;

import org.atlasapi.application.v3.DefaultApplication;
import org.atlasapi.equiv.generators.AliasResolvingEquivalenceGenerator;
import org.atlasapi.equiv.generators.FilmEquivalenceGeneratorAndScorer;
import org.atlasapi.equiv.generators.RadioTimesFilmEquivalenceGenerator;
import org.atlasapi.equiv.results.combining.NullScoreAwareAveragingCombiner;
import org.atlasapi.equiv.results.extractors.AllOverOrEqThresholdExtractor;
import org.atlasapi.equiv.results.extractors.ContinueUntilOneWorksExtractor;
import org.atlasapi.equiv.results.extractors.PercentThresholdEquivalenceExtractor;
import org.atlasapi.equiv.results.filters.ConjunctiveFilter;
import org.atlasapi.equiv.results.filters.DummyContainerFilter;
import org.atlasapi.equiv.results.filters.ExclusionListFilter;
import org.atlasapi.equiv.results.filters.FilmYearFilter;
import org.atlasapi.equiv.results.filters.MediaTypeFilter;
import org.atlasapi.equiv.results.filters.MinimumScoreFilter;
import org.atlasapi.equiv.results.filters.PublisherFilter;
import org.atlasapi.equiv.results.filters.SpecializationFilter;
import org.atlasapi.equiv.results.filters.UnpublishedContentFilter;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.update.ContentEquivalenceResultUpdater;
import org.atlasapi.equiv.update.EquivalenceResultUpdater;
import org.atlasapi.equiv.update.updaters.providers.EquivalenceResultUpdaterProvider;
import org.atlasapi.equiv.update.updaters.providers.EquivalenceUpdaterProviderDependencies;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;

import com.metabroadcast.common.stream.MoreCollectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class RtItemUpdaterProvider implements EquivalenceResultUpdaterProvider<Item> {

    private final Set<Set<String>> namespacesSet;

    private RtItemUpdaterProvider(@Nullable Set<Set<String>> namespacesSet) {
        this.namespacesSet = namespacesSet == null
                             ? ImmutableSet.of()
                             : namespacesSet.stream()
                                     .map(ImmutableSet::copyOf)
                                     .collect(MoreCollectors.toImmutableSet());
    }

    public static RtItemUpdaterProvider create(@Nullable Set<Set<String>> namespacesSet) {
        return new RtItemUpdaterProvider(namespacesSet);
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
                                new RadioTimesFilmEquivalenceGenerator(
                                        dependencies.getContentResolver()
                                ),
                                new FilmEquivalenceGeneratorAndScorer(
                                        dependencies.getOwlSearchResolver(),
                                        targetPublishers,
                                        DefaultApplication.createWithReads(
                                                ImmutableList.copyOf(targetPublishers)
                                        ),
                                        false,
                                        1,
                                        Score.ONE,
                                        Score.negativeOne(),
                                        Score.valueOf(3D),
                                        Score.ZERO
                                ),
                                AliasResolvingEquivalenceGenerator.<Item>builder()
                                        .withResolver(dependencies.getContentResolver())
                                        .withPublishers(targetPublishers)
                                        .withLookupEntryStore(dependencies.getLookupEntryStore())
                                        .withNamespacesSet(namespacesSet)
                                        .withAliasMatchingScore(Score.ONE)
                                        .withIncludeUnpublishedContent(false)
                                        .withClass(Item.class)
                                        .build()
                        )
                )
                .withScorers(
                        ImmutableSet.of()
                )
                .withCombiner(
                        new NullScoreAwareAveragingCombiner<>()
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
                .withExtractor(
                        //prioritise equiv'ing to old PA ID over new PA ID
                        //(
                        ContinueUntilOneWorksExtractor.create(ImmutableList.of(
                                AllOverOrEqThresholdExtractor.create(2.9),
                                AllOverOrEqThresholdExtractor.create(0.9)
                                )
                        )
                )
                .build();
    }
}

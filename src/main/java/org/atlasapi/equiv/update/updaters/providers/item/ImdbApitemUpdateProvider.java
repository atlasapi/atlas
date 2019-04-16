package org.atlasapi.equiv.update.updaters.providers.item;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.atlasapi.application.v3.DefaultApplication;
import org.atlasapi.equiv.generators.FilmEquivalenceGenerator;
import org.atlasapi.equiv.generators.TitleSearchGenerator;
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
import org.atlasapi.equiv.scorers.BarbTitleMatchingItemScorer;
import org.atlasapi.equiv.scorers.ItemYearScorer;
import org.atlasapi.equiv.update.ContentEquivalenceResultUpdater;
import org.atlasapi.equiv.update.EquivalenceResultUpdater;
import org.atlasapi.equiv.update.updaters.providers.EquivalenceResultUpdaterProvider;
import org.atlasapi.equiv.update.updaters.providers.EquivalenceUpdaterProviderDependencies;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;

import java.util.Set;

public class ImdbApitemUpdateProvider implements EquivalenceResultUpdaterProvider<Item> {

    private ImdbApitemUpdateProvider() {

    }

    public static ImdbApitemUpdateProvider create() {
        return new ImdbApitemUpdateProvider();
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
                                new FilmEquivalenceGenerator(
                                        dependencies.getSearchResolver(),
                                        targetPublishers,
                                        DefaultApplication.createWithReads(
                                                ImmutableList.copyOf(targetPublishers)
                                        ),
                                        true),
                                TitleSearchGenerator.create(
                                        dependencies.getSearchResolver(),
                                        Item.class,
                                        targetPublishers,
                                        0,
                                        false,
                                        false
                                )
                        )
                )
                .withScorers(
                        ImmutableSet.of(
                                BarbTitleMatchingItemScorer.builder()
                                        .withScoreOnPerfectMatch(Score.valueOf(2.0))
                                        .withScoreOnMismatch(Score.ZERO)
                                        .build(),
                                new ItemYearScorer(Score.ONE)
                        )
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
                        AllOverOrEqThresholdExtractor.create(3D)
                )
                .build();
    }
}

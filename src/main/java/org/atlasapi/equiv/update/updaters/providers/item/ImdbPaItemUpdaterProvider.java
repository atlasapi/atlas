package org.atlasapi.equiv.update.updaters.providers.item;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.atlasapi.application.v3.DefaultApplication;
import org.atlasapi.equiv.generators.ContainerCandidatesItemEquivalenceGenerator;
import org.atlasapi.equiv.generators.FilmEquivalenceGeneratorAndScorer;
import org.atlasapi.equiv.generators.TitleSearchGenerator;
import org.atlasapi.equiv.results.combining.AddingEquivalenceCombiner;
import org.atlasapi.equiv.results.extractors.AllOverOrEqThresholdExtractor;
import org.atlasapi.equiv.results.filters.ConjunctiveFilter;
import org.atlasapi.equiv.results.filters.DummyContainerFilter;
import org.atlasapi.equiv.results.filters.ExclusionListFilter;
import org.atlasapi.equiv.results.filters.FilmYearFilter;
import org.atlasapi.equiv.results.filters.MediaTypeFilter;
import org.atlasapi.equiv.results.filters.MinimumScoreFilter;
import org.atlasapi.equiv.results.filters.SpecializationFilter;
import org.atlasapi.equiv.results.filters.UnpublishedContentFilter;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.scorers.DescriptionMatchingScorer;
import org.atlasapi.equiv.scorers.DescriptionTitleMatchingScorer;
import org.atlasapi.equiv.scorers.FilmYearScorer;
import org.atlasapi.equiv.scorers.SequenceItemScorer;
import org.atlasapi.equiv.scorers.TitleMatchingItemScorer;
import org.atlasapi.equiv.update.ContentEquivalenceResultUpdater;
import org.atlasapi.equiv.update.EquivalenceResultUpdater;
import org.atlasapi.equiv.update.updaters.providers.EquivalenceResultUpdaterProvider;
import org.atlasapi.equiv.update.updaters.providers.EquivalenceUpdaterProviderDependencies;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;

import java.util.Set;

public class ImdbPaItemUpdaterProvider implements EquivalenceResultUpdaterProvider<Item> {

    private ImdbPaItemUpdaterProvider() {}

    public static ImdbPaItemUpdaterProvider create() {
        return new ImdbPaItemUpdaterProvider();
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
                                new ContainerCandidatesItemEquivalenceGenerator(
                                        dependencies.getContentResolver(),
                                        dependencies.getEquivSummaryStore(),
                                        targetPublishers
                                ),
                                TitleSearchGenerator.create(
                                        dependencies.getSearchResolver(),
                                        Item.class,
                                        targetPublishers,
                                        2, //TitleMatchingItemScorer uses same scoring name
                                        true,
                                        true
                                ),
                                new FilmEquivalenceGeneratorAndScorer(
                                        dependencies.getSearchResolver(),
                                        targetPublishers,
                                        DefaultApplication.createWithReads(
                                                ImmutableList.copyOf(targetPublishers)),
                                        true,
                                        0,
                                        Score.nullScore(),
                                        Score.nullScore(),
                                        Score.nullScore(),
                                        Score.nullScore()
                                )
                        )
                )
                .withScorers(
                        ImmutableSet.of(
                                new TitleMatchingItemScorer(),
                                new SequenceItemScorer(Score.ONE),
                                new FilmYearScorer(Score.ONE, Score.ZERO, Score.ONE),
                                DescriptionTitleMatchingScorer.createItemScorer(),
                                DescriptionMatchingScorer.makeItemScorer()
                        )
                )
                .withCombiner(
                        new AddingEquivalenceCombiner<>()
                )
                .withFilter(
                        ConjunctiveFilter.valueOf(ImmutableList.of(
                                new MinimumScoreFilter<>(0.99),
                                new MediaTypeFilter<>(),
                                new SpecializationFilter<>(),
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
                        AllOverOrEqThresholdExtractor.create(3)
                )
                .build();
    }
}

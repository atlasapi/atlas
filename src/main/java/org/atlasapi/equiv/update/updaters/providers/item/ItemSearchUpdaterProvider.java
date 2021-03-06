package org.atlasapi.equiv.update.updaters.providers.item;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.atlasapi.application.v3.DefaultApplication;
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
import org.atlasapi.equiv.scorers.ItemYearScorer;
import org.atlasapi.equiv.scorers.SequenceItemScorer;
import org.atlasapi.equiv.scorers.SoleCandidateTitleMatchingScorer;
import org.atlasapi.equiv.scorers.TitleMatchingItemScorer;
import org.atlasapi.equiv.update.ContentEquivalenceResultUpdater;
import org.atlasapi.equiv.update.EquivalenceResultUpdater;
import org.atlasapi.equiv.update.updaters.providers.EquivalenceResultUpdaterProvider;
import org.atlasapi.equiv.update.updaters.providers.EquivalenceUpdaterProviderDependencies;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;

import java.util.Set;

/**
 *  Equivs on both exact title IFF candidate is the only one with exact title match from its publisher,
 *  AND the subject is the only one with that exact title from that publisher.
 *  Otherwise, it requires exact year match.
 */
public class ItemSearchUpdaterProvider implements EquivalenceResultUpdaterProvider<Item> {

    private ItemSearchUpdaterProvider() {}

    public static ItemSearchUpdaterProvider create() {
        return new ItemSearchUpdaterProvider();
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
                                TitleSearchGenerator.create(
                                        dependencies.getOwlSearchResolver(),
                                        Item.class,
                                        targetPublishers,
                                        Score.nullScore(),
                                        Score.nullScore(),
                                        true,
                                        true,
                                        true
                                ),
                                TitleSearchGenerator.create(
                                        dependencies.getSherlockSearchResolver(),
                                        Item.class,
                                        targetPublishers,
                                        Score.ZERO,
                                        Score.ZERO,
                                        true,
                                        true,
                                        true
                                ),
                                new FilmEquivalenceGeneratorAndScorer(
                                        dependencies.getOwlSearchResolver(),
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
                                new SoleCandidateTitleMatchingScorer<>(
                                        dependencies.getSherlockSearchResolver(),
                                        Score.ONE,
                                        Score.nullScore(),
                                        Item.class
                                ),
                                new ItemYearScorer(Score.ONE, Score.negativeOne(), Score.nullScore()),
                                new SequenceItemScorer(Score.ONE)
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

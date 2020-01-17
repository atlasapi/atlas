package org.atlasapi.equiv.update.updaters.providers.item.amazon;

import java.util.Set;

import org.atlasapi.application.v3.DefaultApplication;
import org.atlasapi.equiv.generators.ContainerCandidatesItemEquivalenceGenerator;
import org.atlasapi.equiv.generators.FilmEquivalenceGeneratorAndScorer;
import org.atlasapi.equiv.results.combining.AddingEquivalenceCombiner;
import org.atlasapi.equiv.results.combining.RequiredScoreFilteringCombiner;
import org.atlasapi.equiv.results.extractors.PercentThresholdEquivalenceExtractor;
import org.atlasapi.equiv.results.filters.ConjunctiveFilter;
import org.atlasapi.equiv.results.filters.DummyContainerFilter;
import org.atlasapi.equiv.results.filters.ExclusionListFilter;
import org.atlasapi.equiv.results.filters.FilmYearFilter;
import org.atlasapi.equiv.results.filters.MediaTypeFilter;
import org.atlasapi.equiv.results.filters.MinimumScoreFilter;
import org.atlasapi.equiv.results.filters.SpecializationFilter;
import org.atlasapi.equiv.results.filters.UnpublishedContentFilter;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.scorers.FilmYearScorer;
import org.atlasapi.equiv.scorers.SequenceItemScorer;
import org.atlasapi.equiv.scorers.TitleMatchingItemScorer;
import org.atlasapi.equiv.update.ContentEquivalenceResultUpdater;
import org.atlasapi.equiv.update.EquivalenceResultUpdater;
import org.atlasapi.equiv.update.updaters.providers.EquivalenceResultUpdaterProvider;
import org.atlasapi.equiv.update.updaters.providers.EquivalenceUpdaterProviderDependencies;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

// N.B. There is some flip-flopping happening on Amazon-PA equiv that has to do with the sequence
// stitching that happens at container level when equiv is run. This can cause situations such as
// Amazon items equiv'ing to PA items when equiv run on container level, but not equiv'ing when run
// on item level.
public class AmazonItemUpdaterProvider implements EquivalenceResultUpdaterProvider<Item> {

    private AmazonItemUpdaterProvider() {
    }

    public static AmazonItemUpdaterProvider create() {
        return new AmazonItemUpdaterProvider();
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
                        //whatever generators are used here, should prevent the creation of
                        //candidates which are the item itself (because there is no further filtering
                        //to remove them, whereas the Publisher filter used elsewhere does that).
                        ImmutableSet.of(
                                new ContainerCandidatesItemEquivalenceGenerator(
                                        dependencies.getContentResolver(),
                                        dependencies.getEquivSummaryStore(),
                                        targetPublishers
                                ),
                                new FilmEquivalenceGeneratorAndScorer(
                                        dependencies.getOwlSearchResolver(),
                                        targetPublishers,
                                        DefaultApplication.createWithReads(
                                                ImmutableList.copyOf(targetPublishers)),
                                        true,
                                        0, //only equiv on exact year, as agreed
                                        Score.nullScore(), //score with ItemYearScorer
                                        Score.nullScore(),
                                        Score.nullScore(),
                                        Score.nullScore()
                                )
                        )
                )
                .withScorers(
                        ImmutableSet.of(
                                new TitleMatchingItemScorer(), // Scores 2 on exact match.
                                //DescriptionMatchingScorer.makeScorer(), TODO sometimes broken ATM
                                new SequenceItemScorer(Score.ONE),
                                //matches original behaviour of FilmEquivalenceGeneratorAndScorer
                                // scoring, has a 0 year difference tolerance (ENG-144)
                                new FilmYearScorer(Score.ONE, Score.ZERO, Score.ONE)
                        )
                )
                .withCombiner(
                        new RequiredScoreFilteringCombiner<>(
                                new AddingEquivalenceCombiner<>(),
                                TitleMatchingItemScorer.NAME
                        )
                )
                .withFilter(
                        ConjunctiveFilter.valueOf(ImmutableList.of(
                                new MinimumScoreFilter<>(0.25),
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
                        PercentThresholdEquivalenceExtractor.moreThanPercent(90)
                )
                .build();
    }
}

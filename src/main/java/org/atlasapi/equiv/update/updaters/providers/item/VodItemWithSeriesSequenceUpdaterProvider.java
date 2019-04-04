package org.atlasapi.equiv.update.updaters.providers.item;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.atlasapi.application.v3.DefaultApplication;
import org.atlasapi.equiv.generators.ContainerCandidatesItemEquivalenceGenerator;
import org.atlasapi.equiv.generators.FilmEquivalenceGenerator;
import org.atlasapi.equiv.results.combining.NullScoreAwareAveragingCombiner;
import org.atlasapi.equiv.results.combining.RequiredScoreFilteringCombiner;
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
import org.atlasapi.equiv.scorers.SequenceItemScorer;
import org.atlasapi.equiv.scorers.SeriesSequenceItemScorer;
import org.atlasapi.equiv.scorers.TitleMatchingItemScorer;
import org.atlasapi.equiv.update.ContentEquivalenceResultUpdater;
import org.atlasapi.equiv.update.EquivalenceResultUpdater;
import org.atlasapi.equiv.update.updaters.providers.EquivalenceResultUpdaterProvider;
import org.atlasapi.equiv.update.updaters.providers.EquivalenceUpdaterProviderDependencies;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;

import java.util.Set;

public class VodItemWithSeriesSequenceUpdaterProvider implements EquivalenceResultUpdaterProvider<Item> {

    private VodItemWithSeriesSequenceUpdaterProvider() {
    }

    public static VodItemWithSeriesSequenceUpdaterProvider create() {
        return new VodItemWithSeriesSequenceUpdaterProvider();
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
                                        dependencies.getEquivSummaryStore()
                                ),
                                new FilmEquivalenceGenerator(
                                        dependencies.getSearchResolver(),
                                        targetPublishers,
                                        DefaultApplication.createWithReads(
                                                ImmutableList.copyOf(targetPublishers)
                                        ),
                                        true
                                )
                        )
                )
                .withScorers(
                        ImmutableSet.of(
                                new TitleMatchingItemScorer(),
                                new SequenceItemScorer(Score.ONE),
                                new SeriesSequenceItemScorer()
                        )
                )
                .withCombiner(
                        new RequiredScoreFilteringCombiner<>(
                                new NullScoreAwareAveragingCombiner<>(),
                                TitleMatchingItemScorer.NAME
                        )
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
                        PercentThresholdEquivalenceExtractor.moreThanPercent(90)
                )
//                .withHandler(
//                        //TODO: strict
//                        new DelegatingEquivalenceResultHandler<>(ImmutableList.of(
//                                EpisodeFilteringEquivalenceResultHandler.strict(
//                                        LookupWritingEquivalenceHandler.create(
//                                                dependencies.getLookupWriter()
//                                        ),
//                                        dependencies.getEquivSummaryStore()
//                                ),
//                                new ResultWritingEquivalenceHandler<>(
//                                        dependencies.getEquivalenceResultStore()
//                                ),
//                                new EquivalenceSummaryWritingHandler<>(
//                                        dependencies.getEquivSummaryStore()
//                                )
//                        ))
//                )
//                .withMessenger(
//                        //standard
//                        QueueingEquivalenceResultMessenger.create(
//                                dependencies.getMessageSender(),
//                                dependencies.getLookupEntryStore()
//                        )
//                )
                .build();
    }
}

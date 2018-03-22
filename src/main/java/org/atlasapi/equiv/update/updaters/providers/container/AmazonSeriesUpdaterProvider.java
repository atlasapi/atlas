package org.atlasapi.equiv.update.updaters.providers.container;

import java.util.Set;

import org.atlasapi.equiv.generators.ContainerCandidatesContainerEquivalenceGenerator;
import org.atlasapi.equiv.handlers.DelegatingEquivalenceResultHandler;
import org.atlasapi.equiv.handlers.EquivalenceSummaryWritingHandler;
import org.atlasapi.equiv.handlers.LookupWritingEquivalenceHandler;
import org.atlasapi.equiv.handlers.ResultWritingEquivalenceHandler;
import org.atlasapi.equiv.messengers.QueueingEquivalenceResultMessenger;
import org.atlasapi.equiv.results.combining.NullScoreAwareAveragingCombiner;
import org.atlasapi.equiv.results.extractors.AllOverOrEqThresholdExtractor;
import org.atlasapi.equiv.results.extractors.ContinueUntilOneWorksExtractor;
import org.atlasapi.equiv.results.extractors.MultipleCandidateExtractor;
import org.atlasapi.equiv.results.extractors.PercentThresholdEquivalenceExtractor;
import org.atlasapi.equiv.results.extractors.RemoveAndCombineExtractor;
import org.atlasapi.equiv.results.filters.ConjunctiveFilter;
import org.atlasapi.equiv.results.filters.ContainerHierarchyFilter;
import org.atlasapi.equiv.results.filters.DummyContainerFilter;
import org.atlasapi.equiv.results.filters.ExclusionListFilter;
import org.atlasapi.equiv.results.filters.FilmFilter;
import org.atlasapi.equiv.results.filters.MediaTypeFilter;
import org.atlasapi.equiv.results.filters.MinimumScoreFilter;
import org.atlasapi.equiv.results.filters.SpecializationFilter;
import org.atlasapi.equiv.results.filters.UnpublishedContentFilter;
import org.atlasapi.equiv.scorers.SequenceContainerScorer;
import org.atlasapi.equiv.update.ContentEquivalenceUpdater;
import org.atlasapi.equiv.update.EquivalenceUpdater;
import org.atlasapi.equiv.update.updaters.providers.EquivalenceUpdaterProvider;
import org.atlasapi.equiv.update.updaters.providers.EquivalenceUpdaterProviderDependencies;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Publisher;

import com.google.common.collect.ImmutableList;

public class AmazonSeriesUpdaterProvider implements EquivalenceUpdaterProvider<Container> {

    private AmazonSeriesUpdaterProvider() {
    }

    public static AmazonSeriesUpdaterProvider create() {
        return new AmazonSeriesUpdaterProvider();
    }

    @Override
    public EquivalenceUpdater<Container> getUpdater(
            EquivalenceUpdaterProviderDependencies dependencies,
            Set<Publisher> targetPublishers
    ) {
        return ContentEquivalenceUpdater.<Container>builder()
                .withExcludedUris(dependencies.getExcludedUris())
                .withExcludedIds(dependencies.getExcludedIds())
                .withGenerator(
                        //whatever generators are used here, should prevent the creation of
                        //candidates which are the item itself (because there is no further filtering
                        //to remove them, whereas the Publisher filter used elsewhere does that).
                        new ContainerCandidatesContainerEquivalenceGenerator(
                                dependencies.getContentResolver(),
                                dependencies.getEquivSummaryStore()
                        )
                )
                .withScorer(
                        new SequenceContainerScorer()
                )
                .withCombiner(
                        new NullScoreAwareAveragingCombiner<>()
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
                                new FilmFilter<>(),
                                new DummyContainerFilter<>(),
                                new UnpublishedContentFilter<>(),
                                new ContainerHierarchyFilter()
                        ))
                )
                .withExtractors(
                        ImmutableList.of(
                                //get all items that scored perfectly everywhere.
                                //this should equiv all amazon versions of the same content together
                                //then let it equate with other stuff as well.
                                RemoveAndCombineExtractor.create(
                                        AllOverOrEqThresholdExtractor.create(3.0),
                                        ContinueUntilOneWorksExtractor.create(
                                                ImmutableList.of(
                                                        MultipleCandidateExtractor.create(),
                                                        PercentThresholdEquivalenceExtractor.moreThanPercent(
                                                                90)
                                                )
                                        )
                                )
                        )
                )
                .withHandler(
                        new DelegatingEquivalenceResultHandler<>(ImmutableList.of(
                                LookupWritingEquivalenceHandler.create(
                                        dependencies.getLookupWriter()
                                ),
                                new ResultWritingEquivalenceHandler<>(
                                        dependencies.getEquivalenceResultStore()
                                ),
                                new EquivalenceSummaryWritingHandler<>(
                                        dependencies.getEquivSummaryStore()
                                )
                        ))
                )
                .withMessenger(
                        QueueingEquivalenceResultMessenger.create(
                                dependencies.getMessageSender(),
                                dependencies.getLookupEntryStore()
                        )
                )
                .build();
    }
}

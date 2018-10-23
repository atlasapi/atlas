package org.atlasapi.equiv.update.updaters.providers.container;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.atlasapi.equiv.generators.ExactTitleGenerator;
import org.atlasapi.equiv.generators.TitleSearchGenerator;
import org.atlasapi.equiv.handlers.DelegatingEquivalenceResultHandler;
import org.atlasapi.equiv.handlers.EpisodeMatchingEquivalenceHandler;
import org.atlasapi.equiv.handlers.EquivalenceSummaryWritingHandler;
import org.atlasapi.equiv.handlers.LookupWritingEquivalenceHandler;
import org.atlasapi.equiv.handlers.ResultWritingEquivalenceHandler;
import org.atlasapi.equiv.messengers.QueueingEquivalenceResultMessenger;
import org.atlasapi.equiv.results.combining.AddingEquivalenceCombiner;
import org.atlasapi.equiv.results.combining.RequiredScoreFilteringCombiner;
import org.atlasapi.equiv.results.extractors.AllWithTheSameHighscoreAndPublisherExtractor;
import org.atlasapi.equiv.results.extractors.PercentThresholdAboveNextBestMatchEquivalenceExtractor;
import org.atlasapi.equiv.results.extractors.RemoveAndCombineExtractor;
import org.atlasapi.equiv.results.filters.ConjunctiveFilter;
import org.atlasapi.equiv.results.filters.DummyContainerFilter;
import org.atlasapi.equiv.results.filters.ExclusionListFilter;
import org.atlasapi.equiv.results.filters.MediaTypeFilter;
import org.atlasapi.equiv.results.filters.MinimumScoreFilter;
import org.atlasapi.equiv.results.filters.SpecializationFilter;
import org.atlasapi.equiv.results.filters.UnpublishedContentFilter;
import org.atlasapi.equiv.results.scores.ScoreThreshold;
import org.atlasapi.equiv.scorers.TitleMatchingContainerScorer;
import org.atlasapi.equiv.update.ContentEquivalenceUpdater;
import org.atlasapi.equiv.update.EquivalenceUpdater;
import org.atlasapi.equiv.update.updaters.providers.EquivalenceUpdaterProvider;
import org.atlasapi.equiv.update.updaters.providers.EquivalenceUpdaterProviderDependencies;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.mongo.MongoContentResolver;

import java.util.Set;

public class AmazonContainerUpdaterProvider implements EquivalenceUpdaterProvider<Container> {

    private AmazonContainerUpdaterProvider() {
    }

    public static AmazonContainerUpdaterProvider create() {
        return new AmazonContainerUpdaterProvider();
    }

    @Override
    public EquivalenceUpdater<Container> getUpdater(
            EquivalenceUpdaterProviderDependencies dependencies,
            Set<Publisher> targetPublishers
    ) {
        return ContentEquivalenceUpdater.<Container>builder()
                .withExcludedUris(dependencies.getExcludedUris())
                .withExcludedIds(dependencies.getExcludedIds())
                .withGenerators(
//                      whatever generators are used here, should prevent the creation of
//                      candidates which are the item itself (because there is no further filtering
//                      to remove them, whereas the Publisher filter used elsewhere does that).
                        ImmutableSet.of(
                                TitleSearchGenerator.create(
                                        dependencies.getSearchResolver(),
                                        Container.class,
                                        targetPublishers,
                                        2,
                                        true
                                ),
                                new ExactTitleGenerator<>(
                                        dependencies.getMongoContentResolver(),
                                        Container.class,
                                        true,
                                        Publisher.AMAZON_UNBOX
                                )
                        )
                )
                .withScorers(
                        ImmutableSet.of(
                                new TitleMatchingContainerScorer(2)
                        )
                )
                .withCombiner(
                        new RequiredScoreFilteringCombiner<>(
                                new AddingEquivalenceCombiner<>(),
                                TitleMatchingContainerScorer.NAME,
                                ScoreThreshold.greaterThanOrEqual(2)
                        )
                )
                .withFilter(
                        ConjunctiveFilter.valueOf(ImmutableList.of(
                                new MinimumScoreFilter<>(1.99999),
                                new MediaTypeFilter<>(),
                                new SpecializationFilter<>(),
                                ExclusionListFilter.create(
                                        dependencies.getExcludedUris(),
                                        dependencies.getExcludedIds()
                                ),
                                new DummyContainerFilter<>(),
                                new UnpublishedContentFilter<>()
                        ))
                )
                .withExtractors(
                        ImmutableList.of(
                                // Get all amazon items with the same score that scored at least
                                // perfect for title. Then let it equate with other stuff as well.
                                RemoveAndCombineExtractor.create(
                                        AllWithTheSameHighscoreAndPublisherExtractor.create(2.00),
                                        PercentThresholdAboveNextBestMatchEquivalenceExtractor
                                                .atLeastNTimesGreater(1.5)
                                )
                        )
                )
                .withHandler(
                        new DelegatingEquivalenceResultHandler<>(ImmutableList.of(
                                LookupWritingEquivalenceHandler.create(
                                        dependencies.getLookupWriter()
                                ),
                                new EpisodeMatchingEquivalenceHandler(
                                        dependencies.getContentResolver(),
                                        dependencies.getEquivSummaryStore(),
                                        dependencies.getLookupWriter(),
                                        targetPublishers
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

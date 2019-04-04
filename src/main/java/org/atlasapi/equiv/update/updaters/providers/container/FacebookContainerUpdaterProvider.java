package org.atlasapi.equiv.update.updaters.providers.container;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.atlasapi.equiv.generators.AliasResolvingEquivalenceGenerator;
import org.atlasapi.equiv.generators.TitleSearchGenerator;
import org.atlasapi.equiv.results.combining.NullScoreAwareAveragingCombiner;
import org.atlasapi.equiv.results.extractors.MultipleCandidateExtractor;
import org.atlasapi.equiv.results.extractors.PercentThresholdEquivalenceExtractor;
import org.atlasapi.equiv.results.filters.ConjunctiveFilter;
import org.atlasapi.equiv.results.filters.MinimumScoreFilter;
import org.atlasapi.equiv.results.filters.SpecializationFilter;
import org.atlasapi.equiv.results.filters.UnpublishedContentFilter;
import org.atlasapi.equiv.update.ContentEquivalenceResultUpdater;
import org.atlasapi.equiv.update.EquivalenceResultUpdater;
import org.atlasapi.equiv.update.updaters.providers.EquivalenceResultUpdaterProvider;
import org.atlasapi.equiv.update.updaters.providers.EquivalenceUpdaterProviderDependencies;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Publisher;

import java.util.Set;

public class FacebookContainerUpdaterProvider implements EquivalenceResultUpdaterProvider<Container> {

    private FacebookContainerUpdaterProvider() {
    }

    public static FacebookContainerUpdaterProvider create() {
        return new FacebookContainerUpdaterProvider();
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
                                TitleSearchGenerator.create(
                                        dependencies.getSearchResolver(),
                                        Container.class,
                                        targetPublishers,
                                        2
                                ),
                                AliasResolvingEquivalenceGenerator.aliasResolvingGenerator(
                                        dependencies.getContentResolver(),
                                        Container.class
                                )
                ))
                .withScorers(
                        ImmutableSet.of()
                )
                .withCombiner(
                        NullScoreAwareAveragingCombiner.get()
                )
                .withFilter(
                        ConjunctiveFilter.valueOf(ImmutableList.of(
                                new MinimumScoreFilter<>(0.25),
                                new SpecializationFilter<>(),
                                new UnpublishedContentFilter<>()
                        ))
                )
                .withExtractors(
                        ImmutableList.of(
                                MultipleCandidateExtractor.create(),
                                PercentThresholdEquivalenceExtractor.moreThanPercent(90)
                        )
                )
//                .withHandler(
//                        //standard
//                        new DelegatingEquivalenceResultHandler<>(ImmutableList.of(
//                                LookupWritingEquivalenceHandler.create(
//                                        dependencies.getLookupWriter()
//                                ),
//                                new EpisodeMatchingEquivalenceHandler(
//                                        dependencies.getContentResolver(),
//                                        dependencies.getEquivSummaryStore(),
//                                        dependencies.getLookupWriter(),
//                                        targetPublishers
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

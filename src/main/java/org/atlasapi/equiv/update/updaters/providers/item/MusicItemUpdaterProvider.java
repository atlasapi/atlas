package org.atlasapi.equiv.update.updaters.providers.item;

import org.atlasapi.equiv.generators.SongTitleTransform;
import org.atlasapi.equiv.generators.TitleSearchGenerator;
import org.atlasapi.equiv.results.combining.NullScoreAwareAveragingCombiner;
import org.atlasapi.equiv.results.extractors.MusicEquivalenceExtractor;
import org.atlasapi.equiv.results.filters.AlwaysTrueFilter;
import org.atlasapi.equiv.scorers.CrewMemberScorer;
import org.atlasapi.equiv.scorers.SongCrewMemberExtractor;
import org.atlasapi.equiv.update.ContentEquivalenceResultUpdater;
import org.atlasapi.equiv.update.EquivalenceResultUpdater;
import org.atlasapi.equiv.update.updaters.providers.EquivalenceResultUpdaterProvider;
import org.atlasapi.equiv.update.updaters.providers.EquivalenceUpdaterProviderDependencies;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Song;

import java.util.Set;

public class MusicItemUpdaterProvider implements EquivalenceResultUpdaterProvider<Item> {

    private MusicItemUpdaterProvider() {
    }

    public static MusicItemUpdaterProvider create() {
        return new MusicItemUpdaterProvider();
    }

    @Override
    public EquivalenceResultUpdater<Item> getUpdater(
            EquivalenceUpdaterProviderDependencies dependencies, Set<Publisher> targetPublishers
    ) {
        return ContentEquivalenceResultUpdater.<Item>builder()
                .withExcludedUris(dependencies.getExcludedUris())
                .withExcludedIds(dependencies.getExcludedIds())
                .withGenerator(
                        new TitleSearchGenerator<>(
                                dependencies.getSearchResolver(),
                                Song.class,
                                targetPublishers,
                                new SongTitleTransform(),
                                100,
                                2,
                                false
                        )
                )
                .withScorer(
                        new CrewMemberScorer(new SongCrewMemberExtractor())
                )
                .withCombiner(
                        new NullScoreAwareAveragingCombiner<>()
                )
                .withFilter(
                        AlwaysTrueFilter.get()
                )
                .withExtractor(
                        new MusicEquivalenceExtractor()
                )
//                .withHandler(
//                        //standard
//                        new DelegatingEquivalenceResultHandler<>(ImmutableList.of(
//                                EpisodeFilteringEquivalenceResultHandler.relaxed(
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

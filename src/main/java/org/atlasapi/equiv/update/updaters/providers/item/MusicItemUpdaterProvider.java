package org.atlasapi.equiv.update.updaters.providers.item;

import java.util.Set;

import org.atlasapi.equiv.generators.SongTitleTransform;
import org.atlasapi.equiv.generators.TitleSearchGenerator;
import org.atlasapi.equiv.handlers.BroadcastingEquivalenceResultHandler;
import org.atlasapi.equiv.handlers.EpisodeFilteringEquivalenceResultHandler;
import org.atlasapi.equiv.handlers.EquivalenceSummaryWritingHandler;
import org.atlasapi.equiv.handlers.LookupWritingEquivalenceHandler;
import org.atlasapi.equiv.handlers.ResultWritingEquivalenceHandler;
import org.atlasapi.equiv.results.combining.NullScoreAwareAveragingCombiner;
import org.atlasapi.equiv.results.extractors.MusicEquivalenceExtractor;
import org.atlasapi.equiv.results.filters.AlwaysTrueFilter;
import org.atlasapi.equiv.scorers.CrewMemberScorer;
import org.atlasapi.equiv.scorers.SongCrewMemberExtractor;
import org.atlasapi.equiv.update.ContentEquivalenceUpdater;
import org.atlasapi.equiv.update.EquivalenceUpdater;
import org.atlasapi.equiv.update.updaters.providers.EquivalenceUpdaterProvider;
import org.atlasapi.equiv.update.updaters.providers.EquivalenceUpdaterProviderDependencies;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Song;

import com.google.common.collect.ImmutableList;

public class MusicItemUpdaterProvider implements EquivalenceUpdaterProvider<Item> {

    private MusicItemUpdaterProvider() {
    }

    public static MusicItemUpdaterProvider create() {
        return new MusicItemUpdaterProvider();
    }

    @Override
    public EquivalenceUpdater<Item> getUpdater(
            EquivalenceUpdaterProviderDependencies dependencies, Set<Publisher> targetPublishers
    ) {
        return ContentEquivalenceUpdater.<Item>builder()
                .withGenerator(
                        new TitleSearchGenerator<>(
                                dependencies.getSearchResolver(),
                                Song.class,
                                targetPublishers,
                                new SongTitleTransform(),
                                100,
                                2
                        )
                )
                .withScorer(
                        new CrewMemberScorer(new SongCrewMemberExtractor())
                )
                .withExcludedUris(
                        dependencies.getExcludedUris()
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
                .withHandler(
                        new BroadcastingEquivalenceResultHandler<>(ImmutableList.of(
                                EpisodeFilteringEquivalenceResultHandler.relaxed(
                                        new LookupWritingEquivalenceHandler<>(
                                                dependencies.getLookupWriter(),
                                                targetPublishers
                                        ),
                                        dependencies.getEquivSummaryStore()
                                ),
                                new ResultWritingEquivalenceHandler<>(
                                        dependencies.getEquivalenceResultStore()
                                ),
                                new EquivalenceSummaryWritingHandler<>(
                                        dependencies.getEquivSummaryStore()
                                )
                        ))
                )
                .build();
    }
}

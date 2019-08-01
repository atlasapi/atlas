package org.atlasapi.equiv.update.handlers.providers.item;

import com.google.common.collect.ImmutableList;
import org.atlasapi.equiv.handlers.DelegatingEquivalenceResultHandler;
import org.atlasapi.equiv.handlers.EpisodeFilteringEquivalenceResultHandler;
import org.atlasapi.equiv.handlers.EquivalenceResultHandler;
import org.atlasapi.equiv.handlers.EquivalenceSummaryWritingHandler;
import org.atlasapi.equiv.handlers.LookupWritingEquivalenceHandler;
import org.atlasapi.equiv.handlers.ResultWritingEquivalenceHandler;
import org.atlasapi.equiv.update.handlers.providers.EquivalenceResultHandlerProvider;
import org.atlasapi.equiv.update.updaters.providers.EquivalenceUpdaterProviderDependencies;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;

import java.util.Set;

public class StrictEpisodeItemHandlerProvider implements EquivalenceResultHandlerProvider<Item> {

    private StrictEpisodeItemHandlerProvider() {

    }

    public static StrictEpisodeItemHandlerProvider create() {
        return new StrictEpisodeItemHandlerProvider();
    }

    @Override
    public EquivalenceResultHandler<Item> getHandler(
            EquivalenceUpdaterProviderDependencies dependencies,
            Set<Publisher> targetPublishers
    ) {
        return new DelegatingEquivalenceResultHandler<>(ImmutableList.of(
                EpisodeFilteringEquivalenceResultHandler.strict(
                        LookupWritingEquivalenceHandler.create(
                                dependencies.getLookupWriter()
                        ),
                        dependencies.getEquivSummaryStore()
                ),
                new ResultWritingEquivalenceHandler<>(
                        dependencies.getEquivalenceResultStore()
                ),
                new EquivalenceSummaryWritingHandler<>(
                        dependencies.getEquivSummaryStore()
                )
        ));
    }
}

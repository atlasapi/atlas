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

public class StandardItemHandlerProvider implements EquivalenceResultHandlerProvider<Item> {

    private StandardItemHandlerProvider() {

    }

    public static StandardItemHandlerProvider create() {
        return new StandardItemHandlerProvider();
    }

    @Override
    public EquivalenceResultHandler<Item> getHandler(
            EquivalenceUpdaterProviderDependencies dependencies,
            Set<Publisher> targetPublishers
    ) {
        return new DelegatingEquivalenceResultHandler<>(ImmutableList.of(
                EpisodeFilteringEquivalenceResultHandler.relaxed(
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

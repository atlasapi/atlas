package org.atlasapi.equiv.update.handlers.providers.container;

import com.google.common.collect.ImmutableList;
import org.atlasapi.equiv.handlers.DelegatingEquivalenceResultHandler;
import org.atlasapi.equiv.handlers.EpisodeMatchingEquivalenceHandler;
import org.atlasapi.equiv.handlers.EquivalenceResultHandler;
import org.atlasapi.equiv.handlers.EquivalenceSummaryWritingHandler;
import org.atlasapi.equiv.handlers.LookupWritingEquivalenceHandler;
import org.atlasapi.equiv.handlers.ResultWritingEquivalenceHandler;
import org.atlasapi.equiv.update.handlers.providers.EquivalenceResultHandlerProvider;
import org.atlasapi.equiv.update.updaters.providers.EquivalenceUpdaterProviderDependencies;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Publisher;

import java.util.Set;

public class StandardTopLevelContainerHandlerProvider implements EquivalenceResultHandlerProvider<Container> {

    private StandardTopLevelContainerHandlerProvider() {
    }

    public static StandardTopLevelContainerHandlerProvider create() {
        return new StandardTopLevelContainerHandlerProvider();
    }

    @Override
    public EquivalenceResultHandler<Container> getHandler(
            EquivalenceUpdaterProviderDependencies dependencies,
            Set<Publisher> targetPublishers
    ) {
        return new DelegatingEquivalenceResultHandler<>(ImmutableList.of(
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
        ));
    }
}

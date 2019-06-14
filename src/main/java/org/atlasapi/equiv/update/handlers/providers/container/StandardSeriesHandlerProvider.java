package org.atlasapi.equiv.update.handlers.providers.container;

import com.google.common.collect.ImmutableList;
import org.atlasapi.equiv.handlers.DelegatingEquivalenceResultHandler;
import org.atlasapi.equiv.handlers.EquivalenceResultHandler;
import org.atlasapi.equiv.handlers.EquivalenceSummaryWritingHandler;
import org.atlasapi.equiv.handlers.LookupWritingEquivalenceHandler;
import org.atlasapi.equiv.handlers.ResultWritingEquivalenceHandler;
import org.atlasapi.equiv.update.handlers.providers.EquivalenceResultHandlerProvider;
import org.atlasapi.equiv.update.updaters.providers.EquivalenceUpdaterProviderDependencies;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Publisher;

import java.util.Set;

public class StandardSeriesHandlerProvider implements EquivalenceResultHandlerProvider<Container> {

    private StandardSeriesHandlerProvider() {
    }

    public static StandardSeriesHandlerProvider create() {
        return new StandardSeriesHandlerProvider();
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
                new ResultWritingEquivalenceHandler<>(
                        dependencies.getEquivalenceResultStore()
                ),
                new EquivalenceSummaryWritingHandler<>(
                        dependencies.getEquivSummaryStore()
                )
        ));
    }
}

package org.atlasapi.equiv.update.updaters.providers.container;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import org.atlasapi.equiv.results.EquivalenceResult;
import org.atlasapi.equiv.results.description.ReadableDescription;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates;
import org.atlasapi.equiv.update.EquivalenceResultUpdater;
import org.atlasapi.equiv.update.metadata.EquivalenceUpdaterMetadata;
import org.atlasapi.equiv.update.metadata.NopEquivalenceUpdaterMetadata;
import org.atlasapi.equiv.update.updaters.providers.EquivalenceResultUpdaterProvider;
import org.atlasapi.equiv.update.updaters.providers.EquivalenceUpdaterProviderDependencies;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.reporting.telescope.OwlTelescopeReporter;

import java.util.Set;

public class NopContainerUpdaterProvider implements EquivalenceResultUpdaterProvider<Container> {

    private NopContainerUpdaterProvider() {
    }

    public static NopContainerUpdaterProvider create() {
        return new NopContainerUpdaterProvider();
    }

    @Override
    public EquivalenceResultUpdater<Container> getUpdater(
            EquivalenceUpdaterProviderDependencies dependencies,
            Set<Publisher> targetPublishers
    ) {
        return new EquivalenceResultUpdater<Container>() {
            @Override
            public EquivalenceResult<Container> provideEquivalenceResult(
                    Container subject,
                    OwlTelescopeReporter telescope,
                    ReadableDescription desc
            ) {
                return new EquivalenceResult<>(
                        subject,
                        ImmutableList.of(),
                        DefaultScoredCandidates
                                .<Container>fromSource(getClass().getSimpleName())
                                .build(),
                        ImmutableMultimap.of(),
                        desc
                    );
            }

            @Override
            public EquivalenceUpdaterMetadata getMetadata() {
                return NopEquivalenceUpdaterMetadata.create();
            }
        };
    }
}

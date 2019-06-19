package org.atlasapi.equiv.update.updaters.providers.item;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import org.atlasapi.equiv.results.EquivalenceResult;
import org.atlasapi.equiv.results.description.ReadableDescription;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates;
import org.atlasapi.equiv.update.EquivalenceResultUpdater;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResults;
import org.atlasapi.equiv.update.metadata.EquivalenceUpdaterMetadata;
import org.atlasapi.equiv.update.metadata.NopEquivalenceUpdaterMetadata;
import org.atlasapi.equiv.update.updaters.providers.EquivalenceResultUpdaterProvider;
import org.atlasapi.equiv.update.updaters.providers.EquivalenceUpdaterProviderDependencies;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;

import java.util.Set;

public class NopItemUpdaterProvider implements EquivalenceResultUpdaterProvider<Item> {

    private NopItemUpdaterProvider() {
    }

    public static NopItemUpdaterProvider create() {
        return new NopItemUpdaterProvider();
    }

    @Override
    public EquivalenceResultUpdater<Item> getUpdater(
            EquivalenceUpdaterProviderDependencies dependencies,
            Set<Publisher> targetPublishers
    ) {
        return new EquivalenceResultUpdater<Item>() {
            @Override
            public EquivalenceResult<Item> provideEquivalenceResult(
                    Item subject,
                    ReadableDescription desc,
                    EquivToTelescopeResults resultsForTelescope
            ) {
                return new EquivalenceResult<>(
                        subject,
                        ImmutableList.of(),
                        DefaultScoredCandidates
                                .<Item>fromSource(getClass().getSimpleName())
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

package org.atlasapi.equiv.update.updaters.providers.item;

import com.google.common.collect.ImmutableList;
import org.atlasapi.equiv.update.EquivalenceResultUpdater;
import org.atlasapi.equiv.update.FirstMatchingPredicateContentEquivalenceResultUpdater;
import org.atlasapi.equiv.update.updaters.providers.EquivalenceResultUpdaterProvider;
import org.atlasapi.equiv.update.updaters.providers.EquivalenceUpdaterProviderDependencies;
import org.atlasapi.equiv.update.updaters.providers.item.barb.BarbAliasItemUpdaterProvider;
import org.atlasapi.equiv.update.updaters.providers.item.barb.BarbBbcActualTransmissionItemUpdaterProvider;
import org.atlasapi.equiv.update.updaters.providers.item.barb.BarbBbcBroadcastItemUpdaterProvider;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;

import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

public class BbcTxlogsItemUpdaterProvider implements EquivalenceResultUpdaterProvider<Item> {

    private final BarbAliasItemUpdaterProvider barbAliasItemUpdaterProvider;
    private final BarbBbcBroadcastItemUpdaterProvider barbBbcBroadcastItemUpdaterProvider;
    private final BarbBbcActualTransmissionItemUpdaterProvider barbBbcActualTransmissionItemUpdaterProvider;

    private BbcTxlogsItemUpdaterProvider(
            BarbAliasItemUpdaterProvider barbAliasItemUpdaterProvider,
            BarbBbcBroadcastItemUpdaterProvider barbBbcBroadcastItemUpdaterProvider,
            BarbBbcActualTransmissionItemUpdaterProvider barbBbcActualTransmissionItemUpdaterProvider
    ) {
        this.barbAliasItemUpdaterProvider = barbAliasItemUpdaterProvider;
        this.barbBbcBroadcastItemUpdaterProvider = barbBbcBroadcastItemUpdaterProvider;
        this.barbBbcActualTransmissionItemUpdaterProvider = barbBbcActualTransmissionItemUpdaterProvider;
    }

    public static BbcTxlogsItemUpdaterProvider create(
            BarbAliasItemUpdaterProvider barbAliasItemUpdaterProvider,
            BarbBbcBroadcastItemUpdaterProvider barbBbcBroadcastItemUpdaterProvider,
            BarbBbcActualTransmissionItemUpdaterProvider barbBbcActualTransmissionItemUpdaterProvider
    ) {
        return new BbcTxlogsItemUpdaterProvider(
                barbAliasItemUpdaterProvider,
                barbBbcBroadcastItemUpdaterProvider,
                barbBbcActualTransmissionItemUpdaterProvider
        );
    }

    @Override
    public EquivalenceResultUpdater<Item> getUpdater(
            EquivalenceUpdaterProviderDependencies dependencies, Set<Publisher> targetPublishers
    ) {
        // The predicate is assuming that we've found a strong candidate to the one and only targetPublisher
        // In particular this is strictly used for TxLog <-> Nitro equiv
        checkArgument(targetPublishers.size() == 1);

        EquivalenceResultUpdater<Item> barbAliasEquivResultUpdater = barbAliasItemUpdaterProvider.getUpdater(
                dependencies,
                targetPublishers
        );

        EquivalenceResultUpdater<Item> broadcastEquivResultUpdater = barbBbcBroadcastItemUpdaterProvider.getUpdater(
                dependencies,
                targetPublishers
        );

        EquivalenceResultUpdater<Item> barbBbcActualTransmissionEquivResultUpdater =
                barbBbcActualTransmissionItemUpdaterProvider.getUpdater(
                        dependencies,
                        targetPublishers
                );

        return FirstMatchingPredicateContentEquivalenceResultUpdater.create(
                ImmutableList.of(
                        barbAliasEquivResultUpdater,
                        broadcastEquivResultUpdater,
                        barbBbcActualTransmissionEquivResultUpdater
                ),
                itemEquivalenceResult -> !itemEquivalenceResult.strongEquivalences().isEmpty(),
                "NonEmptyStrongEquivalences"
        );
    }
}

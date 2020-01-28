package org.atlasapi.equiv.update.updaters.providers.item.barb;

import com.google.common.collect.ImmutableList;
import org.atlasapi.equiv.update.EquivalenceResultUpdater;
import org.atlasapi.equiv.update.FirstMatchingPredicateContentEquivalenceResultUpdater;
import org.atlasapi.equiv.update.updaters.providers.EquivalenceResultUpdaterProvider;
import org.atlasapi.equiv.update.updaters.providers.EquivalenceUpdaterProviderDependencies;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;

import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * This does not currently work since Nitro content can potentially equiv to multiple txlog pieces of content,
 * some on alias, some on regular broadcast, some on actual transmission time
 * This can lead to flip flopping when using the FirstMatchingPredicateContentEquivalenceResultUpdater
 */
public class BbcTxlogsEfficientItemUpdaterProvider implements EquivalenceResultUpdaterProvider<Item> {

    private final BarbAliasItemUpdaterProvider barbAliasItemUpdaterProvider;
    private final BarbBbcBroadcastItemUpdaterProvider barbBbcBroadcastItemUpdaterProvider;
    private final BarbBbcActualTransmissionItemUpdaterProvider barbBbcActualTransmissionItemUpdaterProvider;

    private BbcTxlogsEfficientItemUpdaterProvider(
            BarbAliasItemUpdaterProvider barbAliasItemUpdaterProvider,
            BarbBbcBroadcastItemUpdaterProvider barbBbcBroadcastItemUpdaterProvider,
            BarbBbcActualTransmissionItemUpdaterProvider barbBbcActualTransmissionItemUpdaterProvider
    ) {
        this.barbAliasItemUpdaterProvider = barbAliasItemUpdaterProvider;
        this.barbBbcBroadcastItemUpdaterProvider = barbBbcBroadcastItemUpdaterProvider;
        this.barbBbcActualTransmissionItemUpdaterProvider = barbBbcActualTransmissionItemUpdaterProvider;
    }

    public static BbcTxlogsEfficientItemUpdaterProvider create(
            BarbAliasItemUpdaterProvider barbAliasItemUpdaterProvider,
            BarbBbcBroadcastItemUpdaterProvider barbBbcBroadcastItemUpdaterProvider,
            BarbBbcActualTransmissionItemUpdaterProvider barbBbcActualTransmissionItemUpdaterProvider
    ) {
        return new BbcTxlogsEfficientItemUpdaterProvider(
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

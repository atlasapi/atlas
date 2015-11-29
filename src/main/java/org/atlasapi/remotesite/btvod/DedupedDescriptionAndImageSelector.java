package org.atlasapi.remotesite.btvod;

import static com.google.common.base.Preconditions.checkNotNull;

import javax.annotation.Nullable;

import org.atlasapi.media.entity.Encoding;
import org.atlasapi.media.entity.Location;
import org.atlasapi.media.entity.Policy;
import org.atlasapi.media.entity.Quality;
import org.atlasapi.media.entity.Version;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;

public class DedupedDescriptionAndImageSelector {

    private static final Ordering<Quality> qualityOrdering =
            Ordering.explicit(Quality.FOUR_K, Quality.HD, Quality.SD).nullsFirst();
    private static final Ordering<Policy.RevenueContract> contractOrdering =
            Ordering.explicit(
                    Policy.RevenueContract.PRIVATE,
                    Policy.RevenueContract.VOLUNTARY_DONATION,
                    Policy.RevenueContract.FREE_TO_VIEW,
                    Policy.RevenueContract.PAY_TO_BUY,
                    Policy.RevenueContract.PAY_TO_RENT,
                    Policy.RevenueContract.SUBSCRIPTION
            )
                    .nullsFirst();
    private static final Ordering<VersionType> versionTypeOrdering =
            Ordering.natural().nullsFirst();
    private static final Function<Location, Policy.RevenueContract> getContract =
            new Function<Location, Policy.RevenueContract>() {

                @Nullable @Override public Policy.RevenueContract apply(@Nullable Location input) {
                    return input != null ? input.getPolicy().getRevenueContract() : null;
                }
            };
    private static final Function<Encoding, VersionType> getVersionType =
            new Function<Encoding, VersionType>() {

                @Nullable @Override public VersionType apply(@Nullable Encoding input) {
                    return input != null ? VersionType.from(input) : null;
                }
            };

    public boolean shouldUpdateDescriptionsAndImages(Iterable<Version> currentVersions,
            Iterable<Version> existingVersions) {
        Optional<VersionType> currentBestOptional = getBestVersionType(currentVersions);
        Optional<VersionType> existingBestOptional = getBestVersionType(existingVersions);

        if (!existingBestOptional.isPresent()) {
            return true;
        }

        if (!currentBestOptional.isPresent()) {
            // We can't decide. Fallback to previous behaviour of keeping the existing fields
            return false;
        }

        VersionType currentBest = currentBestOptional.get();
        VersionType existingBest = existingBestOptional.get();
        return currentBest.compareTo(existingBest) > 0;
    }

    private Optional<VersionType> getBestVersionType(Iterable<Version> versions) {
        // We have to do painful null-checking because we are operating on regular collections
        // and on value objects that provide no non-null guarantees

        Optional<VersionType> bestVersionType = Optional.absent();
        for (Version version : versions) {
            Iterable<VersionType> versionTypes = Iterables.transform(
                    version.getManifestedAs(), getVersionType
            );

            if (!Iterables.isEmpty(versionTypes)) {
                Optional<VersionType> versionTypeOptional = Optional.fromNullable(
                        versionTypeOrdering.max(versionTypes)
                );

                if (versionTypeOptional.isPresent()) {
                    bestVersionType = versionTypeOptional;
                }
            }
        }

        return bestVersionType;
    }

    private static class VersionType implements Comparable<VersionType> {

        private final Quality quality;
        private final Policy.RevenueContract contract;

        private VersionType(Quality quality, Policy.RevenueContract contract) {
            this.quality = checkNotNull(quality);
            this.contract = checkNotNull(contract);
        }

        public static VersionType from(Encoding encoding) {
            Quality quality = encoding.getQuality();
            Iterable<Policy.RevenueContract> contracts = Iterables.transform(
                    encoding.getAvailableAt(), getContract
            );

            if (Iterables.isEmpty(contracts)) {
                return null;
            }
            Policy.RevenueContract contract = contractOrdering.max(contracts);

            if (quality == null || contract == null) {
                return null;
            }

            return new VersionType(quality, contract);
        }

        public Quality getQuality() {
            return quality;
        }

        public Policy.RevenueContract getContract() {
            return contract;
        }

        @Override
        public int compareTo(VersionType that) {
            int qualityComparison = qualityOrdering.compare(this.getQuality(), that.getQuality());
            if (qualityComparison != 0) {
                return qualityComparison;
            }

            return contractOrdering.compare(this.getContract(), that.getContract());
        }
    }
}

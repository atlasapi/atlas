package org.atlasapi.remotesite.btvod;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Encoding;
import org.atlasapi.media.entity.Image;
import org.atlasapi.media.entity.Location;
import org.atlasapi.media.entity.Policy;
import org.atlasapi.media.entity.Quality;
import org.atlasapi.media.entity.Version;
import org.atlasapi.remotesite.btvod.model.BtVodEntry;

import com.google.api.client.repackaged.com.google.common.base.Strings;
import com.google.api.client.util.Maps;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;

public class DedupedDescriptionAndImageUpdater {

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

    private final Map<String, VersionType> descriptionSource = Maps.newHashMap();
    private final Map<String, VersionType> longDescriptionSource = Maps.newHashMap();
    private final Map<String, VersionType> imagesSource = Maps.newHashMap();

    public void updateDescriptionsAndImages(Content target, BtVodEntry source, Set<Image> images,
            Set<Version> currentVersions) {
        Optional<VersionType> currentType = getBestVersionType(currentVersions);

        updateDescription(target, source, currentType);
        updateLongDescription(target, source, currentType);
        updateImages(target, images, currentType);
    }

    private void updateDescription(Content target, BtVodEntry source,
            Optional<VersionType> currentType) {
        if (Strings.isNullOrEmpty(source.getDescription())) {
            return;
        }

        VersionType existingType = descriptionSource.get(target.getCanonicalUri());
        if (hasOwn(target.getDescription(), existingType)) {
            return;
        }

        if (shouldUpdate(target.getDescription(), existingType, currentType)) {
            target.setDescription(source.getDescription());
            descriptionSource.put(target.getCanonicalUri(), currentType.get());
        }
    }

    private void updateLongDescription(Content target, BtVodEntry source,
            Optional<VersionType> currentType) {
        if (Strings.isNullOrEmpty(source.getProductLongDescription())) {
            return;
        }

        VersionType existingType = longDescriptionSource.get(target.getCanonicalUri());
        if (hasOwn(target.getLongDescription(), existingType)) {
            return;
        }

        if (shouldUpdate(target.getLongDescription(), existingType, currentType)) {
            target.setLongDescription(source.getProductLongDescription());
            longDescriptionSource.put(target.getCanonicalUri(), currentType.get());
        }
    }

    private void updateImages(Content target, Set<Image> images,
            Optional<VersionType> currentType) {
        if (images == null || images.isEmpty()) {
            return;
        }

        VersionType existingType = imagesSource.get(target.getCanonicalUri());
        if (hasOwn(target.getImages(), existingType)) {
            return;
        }

        if (shouldUpdate(target.getImages(), existingType, currentType)) {
            target.setImages(images);
            target.setImage(Iterables.get(images, 0).getCanonicalUri());
            imagesSource.put(target.getCanonicalUri(), currentType.get());
        }
    }

    private boolean hasOwn(String fieldValue, VersionType sourceType) {
        return !Strings.isNullOrEmpty(fieldValue) && sourceType == null;
    }

    private boolean shouldUpdate(String fieldValue, VersionType existingType,
            Optional<VersionType> currentType) {
        return currentType.isPresent()
                && (Strings.isNullOrEmpty(fieldValue)
                || currentType.get().compareTo(existingType) > 0);
    }

    private <T> boolean hasOwn(Collection<T> fieldValue, VersionType sourceType) {
        return fieldValue != null && !fieldValue.isEmpty() && sourceType == null;
    }

    private <T> boolean shouldUpdate(Collection<T> fieldValue, VersionType existingType,
            Optional<VersionType> currentType) {
        return currentType.isPresent()
                && (fieldValue == null || fieldValue.isEmpty()
                || currentType.get().compareTo(existingType) > 0);
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

package org.atlasapi.remotesite.btvod;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;

import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Image;
import org.atlasapi.media.entity.Series;
import org.atlasapi.remotesite.btvod.model.BtVodEntry;

import com.google.api.client.util.Sets;
import com.google.common.base.Optional;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Ordering;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import static com.google.common.base.Preconditions.checkNotNull;

public class HierarchyDescriptionAndImageUpdater {

    private final String descriptionGuidAliasNamespace;
    private final String longDescriptionGuidAliasNamespace;
    private final String imagesGuidAliasNamespace;

    public HierarchyDescriptionAndImageUpdater(String descriptionGuidAliasNamespace,
            String longDescriptionGuidAliasNamespace, String imagesGuidAliasNamespace) {
        this.descriptionGuidAliasNamespace = checkNotNull(descriptionGuidAliasNamespace);
        this.longDescriptionGuidAliasNamespace = checkNotNull(longDescriptionGuidAliasNamespace);
        this.imagesGuidAliasNamespace = checkNotNull(imagesGuidAliasNamespace);
    }

    private Map<String, MetadataSource> descriptionSource = new HashMap<>();
    private Map<String, MetadataSource> longDescriptionSource = new HashMap<>();
    private Map<String, MetadataSource> imagesSource = new HashMap<>();

    public void update(Content target, Series series, BtVodEntry seriesRow) {
        Integer seriesNumber = series.getSeriesNumber();
        if (seriesNumber == null) {
            // Set to lowest priority
            seriesNumber = Integer.MAX_VALUE;
        }

        MetadataSource source = MetadataSource.fromSeries(seriesNumber);
        Metadata metadata = Metadata.from(series, seriesRow);
        update(target, metadata, source);
    }

    public void update(Content target, Episode episode, BtVodEntry episodeRow) {
        Integer episodeNumber = episode.getEpisodeNumber();
        if (episodeNumber == null) {
            // Set to lowest priority
            episodeNumber = Integer.MAX_VALUE;
        }

        MetadataSource source = MetadataSource.fromEpisode(episodeNumber);
        Metadata metadata = Metadata.from(episode, episodeRow);
        update(target, metadata, source);
    }

    public void update(Content target, BtVodCollection collection) {
        MetadataSource source = MetadataSource.fromCollection(collection.getCreated());
        Metadata metadata = Metadata.from(collection);
        update(target, metadata, source);
    }

    private void update(Content target, Metadata metadata, MetadataSource source) {
        String existingDescription = target.getDescription();
        String existingLongDescription = target.getLongDescription();
        Set<String> existingImages = Sets.newHashSet();

        if (target.getImages() != null) {
            for (Image image : target.getImages()) {
                existingImages.add(image.getCanonicalUri());
            }
        }

        boolean updatedDescription = updateDescription(
                target, metadata, source, existingDescription
        );
        boolean updatedLongDescription = updateLongDescription(
                target, metadata, source, existingLongDescription
        );
        boolean updatedImages = updateImages(
                target, metadata, source, existingImages
        );

        if (updatedDescription) {
            updateAliases(target, metadata, descriptionGuidAliasNamespace);
        }
        if (updatedLongDescription) {
            updateAliases(target, metadata, longDescriptionGuidAliasNamespace);
        }
        if (updatedImages) {
            updateAliases(target, metadata, imagesGuidAliasNamespace);
        }
    }

    private boolean updateDescription(Content target, Metadata metadata,
            MetadataSource source, String existingDescription) {
        Optional<MetadataSource> existingSource =
                Optional.fromNullable(descriptionSource.get(target.getCanonicalUri()));

        if (hasOwnDescription(target, existingSource)
                || !hasDescription(metadata)
                || metadata.getDescription().equals(existingDescription)) {
            return false;
        }

        if (isHigherPriority(source, existingSource)) {
            target.setDescription(metadata.getDescription());
            descriptionSource.put(target.getCanonicalUri(), source);
            return true;
        }
        return false;
    }

    private boolean hasOwnDescription(Content target, Optional<MetadataSource> sourceOptional) {
        return hasDescription(target) && !sourceOptional.isPresent();
    }

    private boolean hasDescription(Content target) {
        return target.getDescription() != null;
    }

    private boolean hasDescription(Metadata metadata) {
        return metadata.getDescription() != null;
    }

    private boolean updateLongDescription(Content target, Metadata metadata,
            MetadataSource source, String existingLongDescription) {
        Optional<MetadataSource> existingSource =
                Optional.fromNullable(longDescriptionSource.get(target.getCanonicalUri()));

        if (hasOwnLongDescription(target, existingSource)
                || !hasLongDescription(metadata)
                || metadata.getLongDescription().equals(existingLongDescription)) {
            return false;
        }

        if (isHigherPriority(source, existingSource)) {
            target.setLongDescription(metadata.getLongDescription());
            longDescriptionSource.put(target.getCanonicalUri(), source);
            return true;
        }
        return false;
    }

    private boolean hasOwnLongDescription(Content target,
            Optional<MetadataSource> sourceOptional) {
        return hasLongDescription(target) && !sourceOptional.isPresent();
    }

    private boolean hasLongDescription(Content target) {
        return target.getLongDescription() != null;
    }

    private boolean hasLongDescription(Metadata metadata) {
        return metadata.getLongDescription() != null;
    }

    private boolean updateImages(Content target, Metadata metadata,
            MetadataSource source, Set<String> existingImages) {
        Optional<MetadataSource> existingSource =
                Optional.fromNullable(imagesSource.get(target.getCanonicalUri()));

        if (hasOwnImages(target, existingSource) || !hasImages(metadata)) {
            return false;
        }

        Set<String> imageUris = Sets.newHashSet();
        for (Image image : metadata.getImages()) {
            imageUris.add(image.getCanonicalUri());
        }

        if (imageUris.size() == existingImages.size() && imageUris.containsAll(existingImages)) {
            return false;
        }

        if (isHigherPriority(source, existingSource)) {
            target.setImages(metadata.getImages());
            target.setImage(metadata.getImage());
            imagesSource.put(target.getCanonicalUri(), source);
            return true;
        }
        return false;
    }

    private boolean hasOwnImages(Content target, Optional<MetadataSource> sourceOptional) {
        return hasImages(target) && !sourceOptional.isPresent();
    }

    private boolean hasImages(Content target) {
        return target.getImages() != null && !target.getImages().isEmpty();
    }

    private boolean hasImages(Metadata metadata) {
        return metadata.getImages() != null && !metadata.getImages().isEmpty();
    }

    private boolean isHigherPriority(MetadataSource source,
            Optional<MetadataSource> existingSource) {
        // We return true on equality as well because we may see the same source multiple times
        // as different BT VoD entries get deduped in it and each time which images and descriptions
        // have been kept as part of the deduping process may have changed
        return !existingSource.isPresent() || source.compareTo(existingSource.get()) >= 0;
    }

    private void updateAliases(Content target, Metadata metadata, String aliasToUpdate) {
        ImmutableSet.Builder<Alias> aliasBuilder = ImmutableSet.builder();

        for (Alias alias : target.getAliases()) {
            if (!aliasToUpdate.equals(alias.getNamespace())) {
                aliasBuilder.add(alias);
            }
        }

        aliasBuilder.add(new Alias(aliasToUpdate, metadata.getGuid()));

        target.setAliases(aliasBuilder.build());
    }

    private enum MetadataSourceType {
        COLLECTION(1),
        SERIES(2),
        EPISODE(3);

        private final int priority;

        MetadataSourceType(int priority) {
            this.priority = priority;
        }

        public int getPriority() {
            return priority;
        }
    }

    private static class MetadataSource implements Comparable<MetadataSource> {

        private final MetadataSourceType sourceType;

        /**
         * This is a series or episode number if {@link MetadataSource#sourceType} is of
         * type {@link MetadataSourceType#SERIES} or {@link MetadataSourceType#EPISODE}.
         * <p>
         * Alternatively it's an ISO 8601 DateTime if it's of type
         * {@link MetadataSourceType#COLLECTION}
         */
        private final String sourceValue;

        private MetadataSource(MetadataSourceType sourceType, String sourceValue) {
            this.sourceType = sourceType;
            this.sourceValue = sourceValue;
        }

        public static MetadataSource fromCollection(DateTime created) {
            return new MetadataSource(
                    MetadataSourceType.COLLECTION, created.withZone(DateTimeZone.UTC).toString()
            );
        }

        public static MetadataSource fromSeries(int seriesNumber) {
            return new MetadataSource(MetadataSourceType.SERIES, seriesNumber + "");
        }

        public static MetadataSource fromEpisode(int episodeNumber) {
            return new MetadataSource(MetadataSourceType.EPISODE, episodeNumber + "");
        }

        /**
         * We want to take metadata first from a collection, then if that is not available
         * from a series and lastly if that is also not available from an episode.
         * <p>
         * Within each source type we want to take metadata from the series/episode with
         * the lowest series/episode number or from the oldest collection
         */
        @Override
        public int compareTo(@Nonnull MetadataSource that) {
            return ComparisonChain.start()
                    .compare(
                            this.sourceType.getPriority(),
                            that.sourceType.getPriority(),
                            Ordering.natural().reverse()
                    )
                    .compare(
                            this.sourceValue,
                            that.sourceValue,
                            Ordering.<String>natural().reverse()
                    )
                    .result();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            MetadataSource that = (MetadataSource) o;
            return sourceType == that.sourceType &&
                    Objects.equals(sourceValue, that.sourceValue);
        }

        @Override
        public int hashCode() {
            return Objects.hash(sourceType, sourceValue);
        }
    }

    private static class Metadata {

        private final String description;
        private final String longDescription;
        private final Set<Image> images;
        private final String image;
        private final String guid;

        private Metadata(String description, String longDescription, Set<Image> images,
                String image, String guid) {
            this.description = description;
            this.longDescription = longDescription;
            this.images = images;
            this.image = image;
            this.guid = guid;
        }

        public static Metadata from(Content content, BtVodEntry seriesRow) {
            return new Metadata(
                    content.getDescription(),
                    content.getLongDescription(),
                    content.getImages(),
                    content.getImage(),
                    seriesRow.getGuid()
            );
        }

        public static Metadata from(BtVodCollection btVodCollection) {
            return new Metadata(
                    btVodCollection.getDescription(),
                    btVodCollection.getLongDescription(),
                    btVodCollection.getImages(),
                    btVodCollection.getImage(),
                    btVodCollection.getGuid()
            );
        }

        public String getDescription() {
            return description;
        }

        public String getLongDescription() {
            return longDescription;
        }

        public Set<Image> getImages() {
            return images;
        }

        public String getImage() {
            return image;
        }

        public String getGuid() {
            return guid;
        }
    }
}

package org.atlasapi.remotesite.btvod;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Image;
import org.atlasapi.media.entity.Series;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import com.google.common.base.Optional;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Ordering;

public class HierarchyDescriptionAndImageUpdater {

    private Map<String, MetadataSource> descriptionSource = new HashMap<>();
    private Map<String, MetadataSource> longDescriptionSource = new HashMap<>();
    private Map<String, MetadataSource> imagesSource = new HashMap<>();

    public void update(Content target, Series series) {
        Integer seriesNumber = series.getSeriesNumber();
        if (seriesNumber == null) {
            // Set to lowest priority
            seriesNumber = Integer.MAX_VALUE;
        }

        MetadataSource source = MetadataSource.fromSeries(seriesNumber);
        Metadata metadata = Metadata.from(series);
        update(target, metadata, source);
    }

    public void update(Content target, Episode episode) {
        Integer episodeNumber = episode.getEpisodeNumber();
        if (episodeNumber == null) {
            // Set to lowest priority
            episodeNumber = Integer.MAX_VALUE;
        }

        MetadataSource source = MetadataSource.fromEpisode(episodeNumber);
        Metadata metadata = Metadata.from(episode);
        update(target, metadata, source);
    }

    public void update(Content target, BtVodCollection collection) {
        MetadataSource source = MetadataSource.fromCollection(collection.getCreated());
        Metadata metadata = Metadata.from(collection);
        update(target, metadata, source);
    }

    private void update(Content target, Metadata metadata, MetadataSource source) {
        updateDescription(target, metadata, source);
        updateLongDescription(target, metadata, source);
        updateImages(target, metadata, source);
    }

    private void updateDescription(Content target, Metadata metadata, MetadataSource source) {
        Optional<MetadataSource> existingSource =
                Optional.fromNullable(descriptionSource.get(target.getCanonicalUri()));

        if (hasOwnDescription(target, existingSource)) {
            return;
        }

        if (hasDescription(metadata) && isHigherPriority(source, existingSource)) {
            target.setDescription(metadata.getDescription());
            descriptionSource.put(target.getCanonicalUri(), source);
        }
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

    private void updateLongDescription(Content target, Metadata metadata, MetadataSource source) {
        Optional<MetadataSource> existingSource =
                Optional.fromNullable(longDescriptionSource.get(target.getCanonicalUri()));

        if (hasOwnLongDescription(target, existingSource)) {
            return;
        }

        if (hasLongDescription(metadata) && isHigherPriority(source, existingSource)) {
            target.setLongDescription(metadata.getLongDescription());
            longDescriptionSource.put(target.getCanonicalUri(), source);
        }
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

    private void updateImages(Content target, Metadata metadata, MetadataSource source) {
        Optional<MetadataSource> existingSource =
                Optional.fromNullable(imagesSource.get(target.getCanonicalUri()));

        if (hasOwnImages(target, existingSource)) {
            return;
        }

        if (hasImages(metadata) && isHigherPriority(source, existingSource)) {
            target.setImages(metadata.getImages());
            target.setImage(metadata.getImage());
            imagesSource.put(target.getCanonicalUri(), source);
        }
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
        return !existingSource.isPresent() || source.compareTo(existingSource.get()) > 0;
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
        public int compareTo(MetadataSource that) {
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

        private Metadata(String description, String longDescription, Set<Image> images,
                String image) {
            this.description = description;
            this.longDescription = longDescription;
            this.images = images;
            this.image = image;
        }

        public static Metadata from(Content content) {
            return new Metadata(
                    content.getDescription(),
                    content.getLongDescription(),
                    content.getImages(),
                    content.getImage()
            );
        }

        public static Metadata from(BtVodCollection btVodCollection) {
            return new Metadata(
                    btVodCollection.getDescription(),
                    btVodCollection.getLongDescription(),
                    btVodCollection.getImages(),
                    btVodCollection.getImage()
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
    }
}

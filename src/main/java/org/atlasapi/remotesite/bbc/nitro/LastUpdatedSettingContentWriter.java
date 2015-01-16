package org.atlasapi.remotesite.bbc.nitro;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.Clip;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Encoding;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Image;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Location;
import org.atlasapi.media.entity.Policy;
import org.atlasapi.media.entity.Restriction;
import org.atlasapi.media.entity.Version;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.time.Clock;
import com.metabroadcast.common.time.SystemClock;

public class LastUpdatedSettingContentWriter implements ContentWriter {

    private static final Predicate<Identified> HAS_CANONICAL_URI = new Predicate<Identified>() {
        @Override public boolean apply(Identified input) {
            return !Strings.isNullOrEmpty(input.getCanonicalUri());
        }
    };

    private static final Function<Identified, String> TO_CANONICAL_URI = new Function<Identified, String>() {
        @Override public String apply(Identified input) {
            return input.getCanonicalUri();
        }
    };

    private final ContentResolver resolver;
    private final ContentWriter writer;
    private final Clock clock;

    public LastUpdatedSettingContentWriter(ContentResolver resolver, ContentWriter writer, Clock clock) {
        this.resolver = checkNotNull(resolver);
        this.writer = checkNotNull(writer);
        this.clock = checkNotNull(clock);
    }

    public LastUpdatedSettingContentWriter(ContentResolver resolver, ContentWriter writer) {
        this(resolver, writer, new SystemClock());
    }

    @Override
    public void createOrUpdate(Item item) {
        Maybe<Identified> previously = resolver.findByCanonicalUris(ImmutableList.of(item.getCanonicalUri())).get(item.getCanonicalUri());

        DateTime now = clock.now();
        if(previously.hasValue() && previously.requireValue() instanceof Item) {
            Item prevItem = (Item) previously.requireValue();
            if(itemsEqual(prevItem, item) && prevItem.getLastUpdated() != null) {
                item.setLastUpdated(prevItem.getLastUpdated());
            } else {
                item.setLastUpdated(now);
            }
            setUpdatedVersions(prevItem.getVersions(), item.getVersions(), now);
            setUpdatedClips(prevItem.getClips(), item.getClips(), now);
        }
        else {
            item.setLastUpdated(now);
            setUpdatedVersions(Sets.<Version>newHashSet(), item.getVersions(), now);
            setUpdatedClips(Lists.<Clip>newArrayList(), item.getClips(), now);
        }

        writer.createOrUpdate(item);
    }

    private void setUpdatedClips(List<Clip> clips, List<Clip> prevClips, DateTime now) {
        ImmutableMap<String, Clip> prevClipsMap = Maps.uniqueIndex(Iterables.filter(prevClips,
                HAS_CANONICAL_URI), TO_CANONICAL_URI);

        for (Clip clip: clips) {
            Clip prevClip = prevClipsMap.get(clip.getCanonicalUri());

            if (prevClip != null && equal(clip, prevClip) && prevClip.getLastUpdated() != null) {
                clip.setLastUpdated(prevClip.getLastUpdated());
            } else {
                clip.setLastUpdated(now);
            }
        }
    }

    private boolean equal(Clip clip, Clip prevClip) {
        return contentEqual(clip, prevClip)
                && Objects.equal(clip.getClipOf(), prevClip.getClipOf());
    }

    private void setUpdatedVersions(Set<Version> prevVersions, Set<Version> versions, DateTime now) {

        Map<String, Version> prevVersionsMap = prevVersionsMap(prevVersions);
        Map<String, Broadcast> prevBroadcasts = previousBroadcasts(prevVersions);

        for (Version version : versions) {
            Version prevVersion = prevVersionsMap.get(version.getCanonicalUri());
            setLastUpdatedTime(version, prevVersion, now);

            for (Broadcast broadcast : version.getBroadcasts()) {
                Broadcast prevBroadcast = prevBroadcasts.get(broadcast.getSourceId());
                if(prevBroadcast != null && equal(prevBroadcast, broadcast) && prevBroadcast.getLastUpdated() != null) {
                    broadcast.setLastUpdated(prevBroadcast.getLastUpdated());
                } else {
                    broadcast.setLastUpdated(now);
                }
            }

            Set<Encoding> prevEncodings = getPreviousEncodings(prevVersion);

            for (Encoding encoding : version.getManifestedAs()) {
                Optional<Encoding> prevEncoding = Iterables.tryFind(prevEncodings,
                        isEqualTo(encoding));

                setLastUpdatedTime(encoding, prevEncoding, now);
                setLocationsLastUpdatedTime(prevEncoding, encoding.getAvailableAt(), now);
            }
        }
    }

    private void setLastUpdatedTime(Encoding encoding,
            Optional<Encoding> prevEncoding, DateTime now) {
        if (prevEncoding.isPresent() && prevEncoding.get().getLastUpdated() != null) {
            encoding.setLastUpdated(prevEncoding.get().getLastUpdated());
        } else {
            encoding.setLastUpdated(now);
        }
    }

    private Set<Encoding> getPreviousEncodings(Version prevVersion) {
        if (prevVersion != null) {
            return prevVersion.getManifestedAs();
        }

        return Sets.newHashSet();
    }

    private void setLocationsLastUpdatedTime(Optional<Encoding> prevEncoding,
            Set<Location> locations, DateTime now) {

        if (prevEncoding.isPresent()) {
            setLastUpdatedTimeComparingWithPreviousLocations(locations, prevEncoding.get().getAvailableAt(), now);
        } else {
            setLastUpdatedTimeToNow(locations, now);
        }

    }

    private void setLastUpdatedTimeToNow(Set<Location> locations, DateTime now) {
        for (Location location : locations) {
            location.setLastUpdated(now);
        }
    }

    private void setLastUpdatedTimeComparingWithPreviousLocations(Set<Location> locations,
            Set<Location> prevLocations, DateTime now) {
        for (Location location : locations) {
            Optional<Location> prevLocation = Iterables.tryFind(prevLocations, isEqualTo(location));

            if(prevLocation.isPresent() && prevLocation.get().getLastUpdated() != null) {
                location.setLastUpdated(prevLocation.get().getLastUpdated());
            } else {
                location.setLastUpdated(now);
            }
        }
    }

    private Predicate<Encoding> isEqualTo(final Encoding encoding) {
        return new Predicate<Encoding>() {
            @Override
            public boolean apply(Encoding input) {
                return equal(input, encoding);
            }
        };
    }

    private Predicate<Location> isEqualTo(final Location location) {
        return new Predicate<Location>() {
            @Override
            public boolean apply(Location input) {
                return equal(input, location);
            }
        };
    }

    private boolean equal(Encoding encoding, Encoding prevEncoding) {
        return identifiedEqual(encoding, prevEncoding)
                && Objects.equal(encoding.getAdvertisingDuration(), prevEncoding.getAdvertisingDuration())
                && Objects.equal(encoding.getAudioBitRate(), prevEncoding.getAudioBitRate())
                && Objects.equal(encoding.getAudioChannels(), prevEncoding.getAudioChannels())
                && Objects.equal(encoding.getAudioCoding(), prevEncoding.getAudioCoding())
                && Objects.equal(encoding.getAudioDescribed(), prevEncoding.getAudioDescribed())
                && Objects.equal(encoding.getBitRate(), prevEncoding.getBitRate())
                && Objects.equal(encoding.getContainsAdvertising(), prevEncoding.getContainsAdvertising())
                && Objects.equal(encoding.getDataContainerFormat(), prevEncoding.getDataContainerFormat())
                && Objects.equal(encoding.getDataSize(), prevEncoding.getDataSize())
                && Objects.equal(encoding.getDistributor(), prevEncoding.getDistributor())
                && Objects.equal(encoding.getHasDOG(), prevEncoding.getHasDOG())
                && Objects.equal(encoding.getSigned(), prevEncoding.getSigned())
                && Objects.equal(encoding.getSource(), prevEncoding.getSource())
                && Objects.equal(encoding.getVideoAspectRatio(), prevEncoding.getVideoAspectRatio())
                && Objects.equal(encoding.getVideoBitRate(), prevEncoding.getVideoBitRate())
                && Objects.equal(encoding.getVideoCoding(), prevEncoding.getVideoCoding())
                && Objects.equal(encoding.getVideoFrameRate(), prevEncoding.getVideoFrameRate())
                && Objects.equal(encoding.getVideoProgressiveScan(), prevEncoding.getVideoProgressiveScan())
                && Objects.equal(encoding.getVideoVerticalSize(), prevEncoding.getVideoVerticalSize());
    }

    private void setLastUpdatedTime(Version version, Version prevVersion, DateTime now) {
        if (prevVersion != null && equal(prevVersion, version) && prevVersion.getLastUpdated() != null) {
            version.setLastUpdated(prevVersion.getLastUpdated());
        } else {
            version.setLastUpdated(now);
        }
    }

    private Map<String, Version> prevVersionsMap(Set<Version> prevVersions) {
        return Maps.uniqueIndex(Iterables.filter(prevVersions,
                HAS_CANONICAL_URI), TO_CANONICAL_URI);
    }

    private boolean equal(Version prevVersion, Version version) {
        return identifiedEqual(prevVersion, version)
                && equal(prevVersion.getRestriction(), version.getRestriction())
                && Objects.equal(prevVersion.getDuration(), version.getDuration())
                && Objects.equal(prevVersion.getProvider(), version.getProvider())
                && Objects.equal(prevVersion.getPublishedDuration(), version.getPublishedDuration())
                && Objects.equal(prevVersion.getRestriction(), version.getRestriction())
                && Objects.equal(prevVersion.is3d(), version.is3d());
    }

    private boolean equal(Restriction prevRestriction, Restriction restriction) {
        if (prevRestriction == restriction) {
            return true;
        }

        if (prevRestriction == null || restriction == null) {
            return false;
        }

        return identifiedEqual(prevRestriction, restriction)
                && Objects.equal(prevRestriction.isRestricted(), restriction.isRestricted())
                && Objects.equal(prevRestriction.getMessage(), restriction.getMessage())
                && Objects.equal(prevRestriction.getMinimumAge(), restriction.getMinimumAge());
    }

    private boolean equal(Location prevLocation, Location location) {
        if (prevLocation == location) {
            return true;
        }

        if (prevLocation == null || location == null) {
            return false;
        }

        return equal(prevLocation.getPolicy(), location.getPolicy())
                && Objects.equal(prevLocation.getAvailable(), location.getAvailable())
                && Objects.equal(prevLocation.getEmbedCode(), location.getEmbedCode())
                && Objects.equal(prevLocation.getEmbedId(), location.getEmbedId())
                && Objects.equal(prevLocation.getTransportIsLive(), location.getTransportIsLive())
                && Objects.equal(prevLocation.getTransportSubType(), location.getTransportSubType())
                && Objects.equal(prevLocation.getTransportType(), location.getTransportType())
                && Objects.equal(prevLocation.getAliases(), location.getAliases())
                && Objects.equal(prevLocation.getAliasUrls(), location.getAliasUrls())
                && Objects.equal(prevLocation.getAllUris(), location.getAllUris())
                ;
    }

    private boolean equal(Policy prevPolicy, Policy policy) {
        if (prevPolicy == policy) {
            return true;
        }

        if (prevPolicy == null || policy == null) {
            return false;
        }

        return datesEqual(prevPolicy.getAvailabilityStart(), policy.getAvailabilityStart())
                && datesEqual(prevPolicy.getAvailabilityEnd(), policy.getAvailabilityEnd())
                && Objects.equal(prevPolicy.getAvailableCountries(), policy.getAvailableCountries())
                && Objects.equal(prevPolicy.getActualAvailabilityStart(),
                policy.getActualAvailabilityStart())
                && datesEqual(prevPolicy.getDrmPlayableFrom(), policy.getDrmPlayableFrom())
                && Objects.equal(prevPolicy.getNetwork(), policy.getNetwork())
                && Objects.equal(prevPolicy.getPlatform(), policy.getPlatform())
                && Objects.equal(prevPolicy.getPlayer(), policy.getPlayer())
                && Objects.equal(prevPolicy.getPrice(), policy.getPrice())
                && Objects.equal(prevPolicy.getRevenueContract(), policy.getRevenueContract())
                && Objects.equal(prevPolicy.getService(), policy.getService())
                && Objects.equal(prevPolicy.getAliases(), policy.getAliases())
                && Objects.equal(prevPolicy.getAliasUrls(), policy.getAliasUrls())
                ;
    }

    private boolean equal(Broadcast prevBroadcast, Broadcast broadcast) {
        if (prevBroadcast == broadcast) {
            return true;
        }

        if (prevBroadcast == null || broadcast == null) {
            return false;
        }

        return datesEqual(prevBroadcast.getTransmissionTime(), broadcast.getTransmissionTime())
                && datesEqual(prevBroadcast.getTransmissionEndTime(), broadcast.getTransmissionEndTime())
                && datesEqual(prevBroadcast.getActualTransmissionTime(), broadcast.getActualTransmissionTime())
                && datesEqual(prevBroadcast.getActualTransmissionEndTime(), broadcast.getActualTransmissionEndTime())
                && Objects.equal(prevBroadcast.getBroadcastDuration(), broadcast.getBroadcastDuration())
                && Objects.equal(prevBroadcast.isActivelyPublished(), broadcast.isActivelyPublished())
                && Objects.equal(prevBroadcast.getAudioDescribed(), broadcast.getAudioDescribed())
                && Objects.equal(prevBroadcast.getBlackoutRestriction(), broadcast.getBlackoutRestriction())
                && Objects.equal(prevBroadcast.getBroadcastOn(), broadcast.getBroadcastOn())
                && Objects.equal(prevBroadcast.getHighDefinition(), broadcast.getHighDefinition())
                && Objects.equal(prevBroadcast.getLive(), broadcast.getLive())
                && Objects.equal(prevBroadcast.getNewEpisode(), broadcast.getNewEpisode())
                && Objects.equal(prevBroadcast.getNewSeries(), broadcast.getNewSeries())
                && Objects.equal(prevBroadcast.getPremiere(), broadcast.getPremiere())
                && Objects.equal(prevBroadcast.getRepeat(), broadcast.getRepeat())
                && Objects.equal(prevBroadcast.getScheduleDate(), broadcast.getScheduleDate())
                && Objects.equal(prevBroadcast.getSigned(), broadcast.getSigned())
                && Objects.equal(prevBroadcast.getSourceId(), broadcast.getSourceId())
                && Objects.equal(prevBroadcast.getSubtitled(), broadcast.getSubtitled())
                && Objects.equal(prevBroadcast.getSurround(), broadcast.getSurround())
                && Objects.equal(prevBroadcast.getWidescreen(), broadcast.getWidescreen())
                && Objects.equal(prevBroadcast.getAliases(), broadcast.getAliases())
                && Objects.equal(prevBroadcast.getAliasUrls(), broadcast.getAliasUrls())
                ;
    }

    private ImmutableMap<String, Broadcast> previousBroadcasts(Set<Version> prevVersions) {
        Iterable<Broadcast> allBroadcasts = Iterables.concat(Iterables.transform(prevVersions, new Function<Version, Iterable<Broadcast>>() {
            @Override
            public Iterable<Broadcast> apply(Version input) {
                return input.getBroadcasts();
            }
        }));
        return Maps.uniqueIndex(allBroadcasts, new Function<Broadcast, String>() {

            @Override
            public String apply(Broadcast input) {
                return input.getSourceId();
            }
        });
    }

    private boolean itemsEqual(Item prevItem, Item item) {
        return contentEqual(prevItem, item)
                && Objects.equal(item.getPeople(), prevItem.getPeople())
                && Objects.equal(item.getBlackAndWhite(), prevItem.getBlackAndWhite())
                && Objects.equal(item.getContainer(), prevItem.getContainer())
                && Objects.equal(item.getCountriesOfOrigin(), prevItem.getCountriesOfOrigin())
                && Objects.equal(item.getIsLongForm(), prevItem.getIsLongForm())
                ;
    }

    private <T> boolean listsEqualNotCaringOrder(List<T> list1, List<T> list2) {
        if (list1 == null && list2 == null) {
            return true;
        }

        if (list1 == null || list2 == null) {
            return false;
        }

        if (list1.size() != list2.size()) {
            return false;
        }

        return Sets.newHashSet(list1).equals(Sets.newHashSet(list2));
    }

    @Override
    public void createOrUpdate(Container container) {
        Maybe<Identified> previously = resolver.findByCanonicalUris(ImmutableList.of(container.getCanonicalUri())).get(container.getCanonicalUri());

        if(previously.hasValue() && previously.requireValue() instanceof Container) {
            Container prevContainer = (Container) previously.requireValue();
            if(!equal(prevContainer, container)) {
                container.setLastUpdated(clock.now());
                container.setThisOrChildLastUpdated(clock.now());
            }
        }

        if(container.getLastUpdated() == null || previously.isNothing()) {
            container.setLastUpdated(clock.now());
            container.setThisOrChildLastUpdated(clock.now());
        }

        writer.createOrUpdate(container);
    }

    private boolean equal(Image image, Image prevImage) {
        if (image == prevImage) {
            return true;
        }

        if (image == null || prevImage == null) {
            return false;
        }

        return image.equals(prevImage)
                && Objects.equal(image.getAspectRatio(), prevImage.getAspectRatio())
                && datesEqual(image.getAvailabilityEnd(), prevImage.getAvailabilityEnd())
                && datesEqual(image.getAvailabilityStart(), prevImage.getAvailabilityStart())
                && Objects.equal(image.getColor(), prevImage.getColor())
                && Objects.equal(image.getHeight(), prevImage.getHeight())
                && Objects.equal(image.getMimeType(), prevImage.getMimeType())
                && Objects.equal(image.getTheme(), prevImage.getTheme())
                && Objects.equal(image.getType(), prevImage.getType())
                && Objects.equal(image.getWidth(), prevImage.getWidth())
                && Objects.equal(image.getAliases(), prevImage.getAliases())
                && Objects.equal(image.getAliasUrls(), prevImage.getAliasUrls())
                && Objects.equal(image.getAllUris(), prevImage.getAllUris())
                ;
    }

    private boolean equal(Container prevContainer, Container container) {
        return contentEqual(prevContainer, container);
    }

    private boolean identifiedEqual(Identified previous, Identified current) {
        if (previous == current) {
            return true;
        }

        if (previous == null || current == null) {
            return false;
        }

        return Objects.equal(previous.getAliases(), current.getAliases())
                && Objects.equal(previous.getAllUris(), current.getAllUris())
                && Objects.equal(previous.getAliasUrls(), current.getAliasUrls())
                && Objects.equal(previous.getCurie(), current.getCurie());
    }

    private boolean contentEqual(Content prevContent, Content content) {
        return identifiedEqual(prevContent, content)
                && imagesEquals(prevContent.getImages(), content.getImages())
                && listsEqualNotCaringOrder(prevContent.getTopicRefs(), content.getTopicRefs())
                && Objects.equal(prevContent.getTitle(), content.getTitle())
                && Objects.equal(prevContent.getDescription(), content.getDescription())
                && Objects.equal(prevContent.getGenres(), content.getGenres())
                && Objects.equal(prevContent.getImage(), content.getImage())
                && Objects.equal(prevContent.getThumbnail(), content.getThumbnail())
                && Objects.equal(prevContent.getCertificates(), content.getCertificates())
                && Objects.equal(prevContent.getContentGroupRefs(), content.getContentGroupRefs())
                && Objects.equal(prevContent.getKeyPhrases(), content.getKeyPhrases())
                && Objects.equal(prevContent.getLanguages(), content.getLanguages())
                && Objects.equal(prevContent.getLongDescription(), content.getLongDescription())
                && Objects.equal(prevContent.getMediaType(), content.getMediaType())
                && Objects.equal(prevContent.getMediumDescription(), content.getMediumDescription())
                && Objects.equal(prevContent.getPresentationChannel(), content.getPresentationChannel())
                && Objects.equal(prevContent.getPublisher(), content.getPublisher())
                && Objects.equal(prevContent.getRelatedLinks(), content.getRelatedLinks())
                && Objects.equal(prevContent.getReviews(), content.getReviews())
                && Objects.equal(prevContent.getShortDescription(), content.getShortDescription())
                && Objects.equal(prevContent.getSpecialization(), content.getSpecialization())
                && Objects.equal(prevContent.getTags(), content.getTags())
                && Objects.equal(prevContent.getYear(), content.getYear())
                ;
    }

    private boolean imagesEquals(Set<Image> prevImages, Set<Image> images) {
        if (prevImages == images) {
            return true;
        }

        if (prevImages == null || images == null) {
            return false;
        }

        if (prevImages.size() != images.size()) {
            return false;
        }

        for (Image prevImage: prevImages) {
            if (contains(images, prevImage)) {
                return false;
            }
        }

        return true;
    }

    private boolean contains(Set<Image> images, Image prevImage) {
        for (Image image: images) {
            if (equal(image, prevImage)) {
                return true;
            }
        }

        return false;
    }

    private boolean datesEqual(DateTime dateTime1, DateTime dateTime2) {
        if (dateTime1 == dateTime2) {
            return true;
        }

        if (dateTime1 == null || dateTime2 == null) {
            return false;
        }

        return dateTime1.toDateTime(DateTimeZone.UTC)
                .equals(dateTime2.toDateTime(DateTimeZone.UTC));
    }

    private static class EncodingKey {

        private final int horizontalSize;
        private final int verticalSize;

        private EncodingKey(int horizontalSize, int verticalSize) {
            this.horizontalSize = horizontalSize;
            this.verticalSize = verticalSize;
        }

        public int getHorizontalSize() {
            return horizontalSize;
        }

        public int getVerticalSize() {
            return verticalSize;
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof EncodingKey)) {
                return false;
            }

            EncodingKey that = (EncodingKey) other;

            return this.horizontalSize == that.horizontalSize
                    && this.verticalSize == that.verticalSize;
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(horizontalSize, verticalSize);
        }

    }

}

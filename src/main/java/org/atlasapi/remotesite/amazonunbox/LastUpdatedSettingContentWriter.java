package org.atlasapi.remotesite.amazonunbox;

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

import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.time.Clock;
import com.metabroadcast.common.time.SystemClock;

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
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class LastUpdatedSettingContentWriter implements ContentWriter {

    private static final Logger log = LoggerFactory.getLogger(LastUpdatedSettingContentWriter.class);

    private static final Predicate<Identified> HAS_CANONICAL_URI = input -> !Strings.isNullOrEmpty(input.getCanonicalUri());

    private static final Function<Identified, String> TO_CANONICAL_URI = Identified::getCanonicalUri;

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
    public Item createOrUpdate(Item item) {
        Maybe<Identified> previously = resolver.findByCanonicalUris(ImmutableList.of(item.getCanonicalUri())).get(item.getCanonicalUri());

        DateTime now = clock.now();
        if(previously.hasValue() && previously.requireValue() instanceof Item) {
            Item prevItem = (Item) previously.requireValue();
            if(itemsEqual(prevItem, item) && prevItem.getLastUpdated() != null) {
                item.setLastUpdated(prevItem.getLastUpdated());
            } else {
                item.setLastUpdated(now);
            }
            //shouldn't this update the parent last updated?
            setUpdatedVersions(prevItem.getVersions(), item.getVersions(), now);
            setUpdatedClips(prevItem.getClips(), item.getClips(), now);
        }
        else {
            log.info("new item or model change");
            item.setLastUpdated(now);
            setUpdatedVersions(Sets.<Version>newHashSet(), item.getVersions(), now);
            setUpdatedClips(Lists.<Clip>newArrayList(), item.getClips(), now);
        }

        return writer.createOrUpdate(item);
    }

    @Override
    public void createOrUpdate(Container container) {
        Maybe<Identified> previously = resolver.findByCanonicalUris(ImmutableList.of(container.getCanonicalUri())).get(container.getCanonicalUri());

        DateTime now = clock.now();

        if (previously.hasValue() && previously.requireValue() instanceof Container) {
            Container prevContainer = (Container) previously.requireValue();
            if (equal(prevContainer, container) && prevContainer.getThisOrChildLastUpdated() != null) {
                container.setLastUpdated(prevContainer.getThisOrChildLastUpdated());
                container.setThisOrChildLastUpdated(prevContainer.getThisOrChildLastUpdated());
            } else {
                container.setLastUpdated(now);
                container.setThisOrChildLastUpdated(now);
            }
        } else if (container.getLastUpdated() == null) {
            log.info("new container or model change");
            container.setLastUpdated(now);
            container.setThisOrChildLastUpdated(now);
        }

        writer.createOrUpdate(container);
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

                   log.info(broadcast.getId()+" broadcasts {} {} ",prevBroadcast, broadcast);
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
        log.debug("Matching locations: current {}, previous {}", locations, prevLocations);

        for (Location location : locations) {
            Optional<Location> prevLocation = Iterables.tryFind(prevLocations, isEqualTo(location));

            if (prevLocation.isPresent() && prevLocation.get().getLastUpdated() != null) {
                log.debug("Matched location {}", location);
                location.setLastUpdated(prevLocation.get().getLastUpdated());
            } else {
                log.debug("Could not match location {}", location);
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
                && Objects.equal(encoding.getVideoVerticalSize(), prevEncoding.getVideoVerticalSize())
                && Objects.equal(encoding.getSubtitled(), prevEncoding.getSubtitled());
    }

    private void setLastUpdatedTime(Version version, Version prevVersion, DateTime now) {
        if(!equal(prevVersion, version)){log.info(version.getId()+" versions {} {} ",prevVersion, version);}
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
        if( !Objects.equal(item.getPeople(), prevItem.getPeople() )) {log.info(prevItem.getId()+"people {} {} ",item.getPeople(), prevItem.getPeople() );}
        if( !Objects.equal(item.getBlackAndWhite(), prevItem.getBlackAndWhite())){log.info(prevItem.getId()+"black and white {} {} ", item.getBlackAndWhite(), prevItem.getBlackAndWhite());}
        if( !Objects.equal(item.getContainer(), prevItem.getContainer())){log.info(prevItem.getId()+"item containers {} {} ",item.getContainer(), prevItem.getContainer());}
        if( !Objects.equal(item.getCountriesOfOrigin(), prevItem.getCountriesOfOrigin()) ){log.info(prevItem.getId()+"countries of origin {} {} ",item.getCountriesOfOrigin(), prevItem.getCountriesOfOrigin());}
        if( !Objects.equal(item.getIsLongForm(), prevItem.getIsLongForm())){log.info(prevItem.getId()+"is long form {} {} ",item.getIsLongForm(), prevItem.getIsLongForm());}

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


        if( !Objects.equal(current.getAliases(), previous.getAliases())){log.info(previous.getId()+"getAliases {} {} ",current.getAliases(), previous.getAliases());}
        if( !Objects.equal(current.getAllUris(), previous.getAllUris()) ){log.info(previous.getId()+"getAllUris {} {} ",current.getAllUris(), previous.getAllUris());}
        if( !Objects.equal(current.getAliasUrls(), previous.getAliasUrls())){log.info(previous.getId()+"getAliasUrls {} {} ",current.getAliasUrls(), previous.getAliasUrls());}
        if( !Objects.equal(current.getCurie(), previous.getCurie())){log.info(previous.getId()+"getCurie {} {} ",current.getCurie(), previous.getCurie());}


        return Objects.equal(previous.getAliases(), current.getAliases())
                && Objects.equal(previous.getAllUris(), current.getAllUris())
                && Objects.equal(previous.getAliasUrls(), current.getAliasUrls())
                && Objects.equal(previous.getCurie(), current.getCurie());
    }

    private boolean contentEqual(Content prevContent, Content content) {

        if(!imagesEquals(prevContent.getImages(), content.getImages())){log.info(prevContent.getId()+"images {} {}",prevContent.getImages(), content.getImages());}
        if( !              listsEqualNotCaringOrder(prevContent.getTopicRefs(), content.getTopicRefs())){log.info(prevContent.getId()+"topic refs  {} {}");}
        if( !              Objects.equal(prevContent.getTitle(), content.getTitle())){log.info(prevContent.getId()+"title  {} {}",prevContent.getTitle(), content.getTitle());}
        if(  !             Objects.equal(prevContent.getDescription(), content.getDescription())){log.info(prevContent.getId()+"desc {} {}",prevContent.getDescription(), content.getDescription());}
        if(   !            Objects.equal(prevContent.getGenres(), content.getGenres())){log.info(prevContent.getId()+"genre {} {}",prevContent.getGenres(), content.getGenres());}
        if(    !           Objects.equal(prevContent.getImage(), content.getImage())){log.info(prevContent.getId()+"image {} {}",prevContent.getImage(), content.getImage());}
        if(     !          Objects.equal(prevContent.getThumbnail(), content.getThumbnail())){log.info(prevContent.getId()+"thumbnail {} {}",prevContent.getThumbnail(), content.getThumbnail());}
        if(      !         Objects.equal(prevContent.getCertificates(), content.getCertificates())){log.info(prevContent.getId()+"certificates {} {}",prevContent.getCertificates(), content.getCertificates());}
        if(       !        Objects.equal(prevContent.getContentGroupRefs(), content.getContentGroupRefs())){log.info(prevContent.getId()+"content group refs {} {}",prevContent.getContentGroupRefs(), content.getContentGroupRefs());}
        if(        !       Objects.equal(prevContent.getKeyPhrases(), content.getKeyPhrases())){log.info(prevContent.getId()+"key phrases {} {}",prevContent.getKeyPhrases(), content.getKeyPhrases());}
        if(         !      Objects.equal(prevContent.getLanguages(), content.getLanguages())){log.info(prevContent.getId()+"languages {} {}",prevContent.getLanguages(), content.getLanguages());}
        if(          !     Objects.equal(prevContent.getLongDescription(), content.getLongDescription())){log.info(prevContent.getId()+"long desc {} {}",prevContent.getLongDescription(), content.getLongDescription());}
        if(           !    Objects.equal(prevContent.getMediaType(), content.getMediaType())){log.info(prevContent.getId()+"media type {} {}",prevContent.getMediaType(), content.getMediaType());}
        if(            !   Objects.equal(prevContent.getMediumDescription(), content.getMediumDescription())){log.info(prevContent.getId()+"medium desc {} {}",prevContent.getMediumDescription(), content.getMediumDescription());}
        if(             !  Objects.equal(prevContent.getPresentationChannel(), content.getPresentationChannel())){log.info(prevContent.getId()+"presentation channel {} {}",prevContent.getPresentationChannel(), content.getPresentationChannel());}
        if(              ! Objects.equal(prevContent.getPublisher(), content.getPublisher())){log.info(prevContent.getId()+"publisher {} {}",prevContent.getPublisher(), content.getPublisher());}
        if(               !Objects.equal(prevContent.getRelatedLinks(), content.getRelatedLinks())){log.info(prevContent.getId()+"related links {} {}",prevContent.getRelatedLinks(), content.getRelatedLinks());}
        if( !              Objects.equal(prevContent.getReviews(), content.getReviews())){log.info(prevContent.getId()+"reviews {} {}",prevContent.getReviews(), content.getReviews());}
        if(  !             Objects.equal(prevContent.getShortDescription(), content.getShortDescription())){log.info(prevContent.getId()+"short desc {} {}",prevContent.getShortDescription(), content.getShortDescription());}
        if(   !            Objects.equal(prevContent.getSpecialization(), content.getSpecialization())){log.info(prevContent.getId()+"specialtions {} {}",prevContent.getSpecialization(), content.getSpecialization());}
        if(    !           Objects.equal(prevContent.getTags(), content.getTags())){log.info(prevContent.getId()+"tags {} {}",prevContent.getTags(), content.getTags());}
        if(     !          Objects.equal(prevContent.getYear(), content.getYear())){log.info(prevContent.getId()+"years {} {}",prevContent.getYear(), content.getYear());}



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
            if (!contains(images, prevImage)) {
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

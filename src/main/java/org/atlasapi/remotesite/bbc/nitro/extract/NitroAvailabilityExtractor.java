package org.atlasapi.remotesite.bbc.nitro.extract;

import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;
import javax.xml.datatype.XMLGregorianCalendar;

import org.atlasapi.media.TransportType;
import org.atlasapi.media.entity.Encoding;
import org.atlasapi.media.entity.Location;
import org.atlasapi.media.entity.Policy;
import org.atlasapi.media.entity.Policy.Network;
import org.atlasapi.media.entity.Policy.Platform;

import com.metabroadcast.atlas.glycerin.model.Availability;
import com.metabroadcast.atlas.glycerin.model.AvailableVersions;
import com.metabroadcast.atlas.glycerin.model.ScheduledTime;
import com.metabroadcast.common.intl.Countries;
import com.metabroadcast.common.time.DateTimeZones;

import com.google.common.base.Equivalence;
import com.google.common.base.Equivalence.Wrapper;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.joda.time.DateTime;
import org.joda.time.chrono.ISOChronology;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Possibly extracts an {@link Encoding} and {@link Location}s for it from some
 * {@link Availability}s.
 */
public class NitroAvailabilityExtractor {

    private static final LocationEquivalence LOCATION_EQUIVALENCE = new LocationEquivalence();

    private static final Function<Location, Equivalence.Wrapper<Location>> TO_WRAPPED_LOCATION
            = new Function<Location, Equivalence.Wrapper<Location>>() {

        @Override
        public Wrapper<Location> apply(Location input) {
            return LOCATION_EQUIVALENCE.wrap(input);
        }
    };

    private static final Function<Equivalence.Wrapper<Location>, Location> UNWRAP_LOCATION
            = new Function<Equivalence.Wrapper<Location>, Location>() {

        @Override
        public Location apply(Equivalence.Wrapper<Location> input) {
            return input.get();
        }
    };

    private static final String IPLAYER_URL_BASE = "http://www.bbc.co.uk/iplayer/episode/";
    private static final String APPLE_IPHONE4_IPAD_HLS_3G = "apple-iphone4-ipad-hls-3g";
    private static final String APPLE_IPHONE4_HLS = "apple-iphone4-hls";
    private static final String PC = "pc";
    private static final String YOUVIEW = "iptv-all";
    private static final String AVAILABLE = "available";
    private static final String REVOKED = "revoked";
    private static final int HD_HORIZONTAL_SIZE = 1280;
    private static final int HD_VERTICAL_SIZE = 720;

    private static final int SD_HORIZONTAL_SIZE = 640;
    private static final int SD_VERTICAL_SIZE = 360;

    private static final int HD_BITRATE = 3_200_000;
    private static final int SD_BITRATE = 1_500_000;

    private static final String VIDEO_MEDIA_TYPE = "Video";

    private static final Predicate<Availability> IS_HD = new Predicate<Availability>() {
        @Override
        public boolean apply(Availability input) {
            return !input.getMediaSet().contains("iptv-sd");
        }
    };

    private static final Predicate<AvailableVersions.Version.Availabilities.Availability> MIXIN_IS_HD = new Predicate<AvailableVersions.Version.Availabilities.Availability>() {
        @Override
        public boolean apply(AvailableVersions.Version.Availabilities.Availability input) {
            for (AvailableVersions.Version.Availabilities.Availability.MediaSets.MediaSet mediaSet : input
                    .getMediaSets()
                    .getMediaSet()) {
                if ("iptv-sd".equals(mediaSet.getName())) {
                    return true;
                }
            }

            return false;
        }
    };

    private static final Predicate<Availability> IS_SUBTITLED = new Predicate<Availability>() {
        @Override
        public boolean apply(Availability input) {
            return input.getMediaSet().contains("captions");
        }
    };

    private static final Predicate<AvailableVersions.Version.Availabilities.Availability> MIXIN_IS_SUBTITLED = new Predicate<AvailableVersions.Version.Availabilities.Availability>() {
        @Override
        public boolean apply(AvailableVersions.Version.Availabilities.Availability input) {
            AvailableVersions.Version.Availabilities.Availability.MediaSets mediaSets = input.getMediaSets();
            for (AvailableVersions.Version.Availabilities.Availability.MediaSets.MediaSet ms : mediaSets.getMediaSet()) {
                if ("captions".equals(ms.getName())) {
                    return true;
                }
            }

            return false;
        }
    };

    private static final Predicate<Availability> IS_AVAILABLE = new Predicate<Availability>() {
        @Override
        public boolean apply(Availability input) {
            return AVAILABLE.equals(input.getStatus());
        }
    };

    private static final Predicate<AvailableVersions.Version.Availabilities.Availability> MIXIN_IS_AVAILABLE = new Predicate<AvailableVersions.Version.Availabilities.Availability>() {
        @Override
        public boolean apply(AvailableVersions.Version.Availabilities.Availability input) {
            return AVAILABLE.equals(input.getStatus());
        }
    };

    private static final Predicate<Availability> IS_IPTV = new Predicate<Availability>() {
        @Override
        public boolean apply(Availability input) {
            return input.getMediaSet().contains("iptv-all");
        }
    };

    private static final Predicate<AvailableVersions.Version.Availabilities.Availability> MIXIN_IS_IPTV = new Predicate<AvailableVersions.Version.Availabilities.Availability>() {
        @Override
        public boolean apply(AvailableVersions.Version.Availabilities.Availability input) {
            for (AvailableVersions.Version.Availabilities.Availability.MediaSets.MediaSet mediaSet : input
                    .getMediaSets()
                    .getMediaSet()) {
                if ("iptv-all".equals(mediaSet.getName())) {
                    return true;
                }
            }

            return false;
        }
    };

    private final Map<String, Platform> mediaSetPlatform = ImmutableMap.of(
            PC, Platform.PC,
            APPLE_IPHONE4_HLS, Platform.IOS,
            APPLE_IPHONE4_IPAD_HLS_3G, Platform.IOS,
            YOUVIEW, Platform.YOUVIEW_IPLAYER
    );

    private final Map<String, Network> mediaSetNetwork = ImmutableMap.of(
            APPLE_IPHONE4_HLS, Network.WIFI,
            APPLE_IPHONE4_IPAD_HLS_3G, Network.THREE_G
    );

    public Set<Encoding> extractFromMixin(
            String programmePid,
            Iterable<AvailableVersions.Version.Availabilities.Availability> availabilities,
            String mediaType
    ) {
        Set<Equivalence.Wrapper<Location>> hdLocations = Sets.newHashSet();
        Set<Equivalence.Wrapper<Location>> sdLocations = Sets.newHashSet();

        boolean isSubtitled = Iterables.any(
                availabilities,
                Predicates.and(MIXIN_IS_SUBTITLED, MIXIN_IS_AVAILABLE)
        );

        for (AvailableVersions.Version.Availabilities.Availability availability : availabilities) {
            ImmutableList<Wrapper<Location>> locations =
                    FluentIterable.from(getLocationsFor(programmePid, availability, mediaType))
                            .transform(TO_WRAPPED_LOCATION)
                            .toList();

            if (MIXIN_IS_IPTV.apply(availability) && MIXIN_IS_HD.apply(availability)) {
                hdLocations.addAll(locations);
            } else {
                sdLocations.addAll(locations);
            }
        }
        // only create encodings if locations are present for HD/SD
        if (hdLocations.isEmpty()) {
            if (sdLocations.isEmpty()) {
                return ImmutableSet.of();
            }
            return ImmutableSet.of(createEncoding(false, isSubtitled, sdLocations));
        }
        if (sdLocations.isEmpty()) {
            return ImmutableSet.of(createEncoding(true, isSubtitled, hdLocations));
        }
        return ImmutableSet.of(createEncoding(true, isSubtitled, hdLocations),
                createEncoding(false, isSubtitled, sdLocations));
    }

    /**
     * This simplifies the Availability extraction, by assuming that the only difference for an
     * encoding is whether there are HD or SD availabilities. Thus, this creates a maximum of
     * two Encodings, one for SD, one for HD, then maps and dedupes the Locations generated from
     * the availabilities onto those Encodings.
     * <p>
     * The provided collection of {@link Availability} must all be from the same version.
     */
    public Set<Encoding> extract(Iterable<Availability> availabilities, String mediaType) {
        Set<Equivalence.Wrapper<Location>> hdLocations = Sets.newHashSet();
        Set<Equivalence.Wrapper<Location>> sdLocations = Sets.newHashSet();

        boolean isSubtitled = Iterables.any(availabilities,
                Predicates.and(IS_SUBTITLED, IS_AVAILABLE));

        for (Availability availability : availabilities) {
            ImmutableList<Wrapper<Location>> locations = FluentIterable.
                    from(getLocationsFor(availability, mediaType))
                    .transform(TO_WRAPPED_LOCATION)
                    .toList();

            if (IS_IPTV.apply(availability) && IS_HD.apply(availability)) {
                hdLocations.addAll(locations);
            } else {
                sdLocations.addAll(locations);
            }
        }
        // only create encodings if locations are present for HD/SD
        if (hdLocations.isEmpty()) {
            if (sdLocations.isEmpty()) {
                return ImmutableSet.of();
            }
            return ImmutableSet.of(createEncoding(false, isSubtitled, sdLocations));
        }
        if (sdLocations.isEmpty()) {
            return ImmutableSet.of(createEncoding(true, isSubtitled, hdLocations));
        }
        return ImmutableSet.of(createEncoding(true, isSubtitled, hdLocations),
                createEncoding(false, isSubtitled, sdLocations));
    }

    private Encoding createEncoding(boolean isHd, boolean isSubtitled,
            Set<Equivalence.Wrapper<Location>> wrappedLocations) {
        Encoding encoding = new Encoding();

        setHorizontalAndVerticalSize(encoding, isHd);
        encoding.setVideoBitRate(isHd ? HD_BITRATE : SD_BITRATE);

        ImmutableSet<Location> locations = FluentIterable.from(wrappedLocations)
                .transform(UNWRAP_LOCATION)
                .toSet();

        encoding.setAvailableAt(locations);
        encoding.setSubtitled(isSubtitled);

        return encoding;
    }

    private void setHorizontalAndVerticalSize(Encoding encoding, boolean isHD) {
        encoding.setVideoHorizontalSize(isHD ? HD_HORIZONTAL_SIZE : SD_HORIZONTAL_SIZE);
        encoding.setVideoVerticalSize(isHD ? HD_VERTICAL_SIZE : SD_VERTICAL_SIZE);
    }

    private Set<Location> getLocationsFor(Availability availability, String mediaType) {
        ImmutableSet.Builder<Location> locations = ImmutableSet.builder();

        for (String mediaSet : availability.getMediaSet()) {
            Platform platform = mediaSetPlatform.get(mediaSet);
            if (platform != null) {
                locations.add(newLocation(availability, platform,
                        mediaSetNetwork.get(mediaSet), mediaType));
            }
        }
        return locations.build();
    }

    private Set<Location> getLocationsFor(
            String programmePid,
            AvailableVersions.Version.Availabilities.Availability availability,
            String mediaType
    ) {
        ImmutableSet.Builder<Location> locations = ImmutableSet.builder();

        AvailableVersions.Version.Availabilities.Availability.MediaSets mediaSets = availability.getMediaSets();

        for (AvailableVersions.Version.Availabilities.Availability.MediaSets.MediaSet ms : mediaSets.getMediaSet()) {
            Platform platform = mediaSetPlatform.get(ms.getName());

            if (platform != null) {
                locations.add(newLocation(
                        programmePid,
                        availability,
                        ms,
                        platform,
                        mediaSetNetwork.get(ms.getName()),
                        mediaType
                ));
            }
        }

        return locations.build();
    }

    private Location newLocation(
            Availability source,
            Platform platform,
            Network network,
            String mediaType
    ) {
        Location location = new Location();

        location.setUri(IPLAYER_URL_BASE + checkNotNull(NitroUtil.programmePid(source)));
        location.setTransportType(TransportType.LINK);
        location.setPolicy(policy(source, platform, network, mediaType));
        location.setAvailable(!REVOKED.equals(source.getRevocationStatus()));

        return location;
    }

    private Location newLocation(
            String programmePid,
            AvailableVersions.Version.Availabilities.Availability availability,
            AvailableVersions.Version.Availabilities.Availability.MediaSets.MediaSet mediaSet,
            Platform platform,
            Network network,
            String mediaType
    ) {
        Location location = new Location();

        location.setUri(IPLAYER_URL_BASE + checkNotNull(programmePid));
        location.setTransportType(TransportType.LINK);
        location.setPolicy(policy(availability, mediaSet, platform, network, mediaType));
        location.setAvailable(!REVOKED.equals(availability.getStatus()));

        return location;
    }

    private Policy policy(Availability source, Platform platform, Network network,
            String mediaType) {
        Policy policy = new Policy();
        ScheduledTime scheduledTime = source.getScheduledTime();
        if (scheduledTime != null) {
            policy.setAvailabilityStart(toDateTime(scheduledTime.getStart()));
            policy.setAvailabilityEnd(toDateTime(scheduledTime.getEnd()));
        }

        if (shouldIngestActualAvailabilityStart(source, mediaType, policy)) {
            policy.setActualAvailabilityStart(toDateTime(source.getActualStart()));
        }
        policy.setPlatform(platform);
        policy.setNetwork(network);
        policy.setAvailableCountries(ImmutableSet.of(Countries.GB));
        return policy;
    }

    private Policy policy(
            AvailableVersions.Version.Availabilities.Availability source,
            AvailableVersions.Version.Availabilities.Availability.MediaSets.MediaSet mediaSet,
            Platform platform,
            Network network,
            String mediaType
    ) {
        Policy policy = new Policy();

        policy.setAvailabilityStart(toDateTime(source.getScheduledStart()));
        policy.setAvailabilityEnd(toDateTime(source.getScheduledEnd()));
        policy.setActualAvailabilityStart(toDateTime(mediaSet.getActualStart()));

        policy.setPlatform(platform);
        policy.setNetwork(network);
        policy.setAvailableCountries(ImmutableSet.of(Countries.GB));

        return policy;
    }

    private boolean shouldIngestActualAvailabilityStart(Availability source, String mediaType,
            Policy policy) {
        DateTime actualStart = toDateTime(source.getActualStart());

        if (actualStart == null) {
            // Ensures we remove it if not set in Nitro
            return true;
        }

        if (REVOKED.equals(source.getRevocationStatus())) {
            // A revoked availability isn't actually available
            return false;
        }

        if (actualStart.isAfterNow()) {
            // This is not an expected case on the Nitro API, and would cause 
            // problems in output feeds that make relative date checks to set a 
            // "media available" flag, since content would not change when
            // the media becomes available
            return false;
        }

        if (policy.getAvailabilityEnd() != null
                && policy.getAvailabilityEnd().isBeforeNow()) {
            // If we've passed the end of the availability window then ingest 
            // the actual availability start for reference, since there's the 
            // possibility of having missed it if we never ingested during the 
            // availability window.
            return true;
        }

        if (!VIDEO_MEDIA_TYPE.equals(mediaType)) {
            // Since media type is only reliably set on video, not audio, 
            // our condition is inverted to check for this being a radio asset

            // Radio assets are delivered by a range of systems, which don't 
            // reliably record an actual availability start time. However,
            // Nitro synthesises it, and also has logic in the setting of
            // the status field to determine if assets are actually available.
            //
            // Therefore radio actual availability start can only be trusted
            // (and should only be ingested into Atlas) iff status == available
            return AVAILABLE.equals(source.getStatus());
        } else {
            // Video assets' actual availability start time is reliably set
            // in Nitro, since all assets are delivered by video factory which
            // does so. Therefore actual availability start can always be trusted
            // when on video content
            return true;
        }
    }

    private @Nullable DateTime toDateTime(@Nullable XMLGregorianCalendar start) {
        if (start == null) {
            return null;
        }
        return new DateTime(start.toGregorianCalendar(), ISOChronology.getInstance())
                .toDateTime(DateTimeZones.UTC);
    }

}

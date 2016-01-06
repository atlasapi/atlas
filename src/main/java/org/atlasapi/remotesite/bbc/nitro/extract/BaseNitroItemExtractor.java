package org.atlasapi.remotesite.bbc.nitro.extract;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.Encoding;
import org.atlasapi.media.entity.Film;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Location;
import org.atlasapi.media.entity.MediaType;
import org.atlasapi.media.entity.Restriction;
import org.atlasapi.media.entity.Specialization;
import org.atlasapi.media.entity.Version;
import org.atlasapi.remotesite.bbc.BbcFeeds;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.ImmutableSetMultimap.Builder;
import com.google.common.collect.Iterables;
import com.metabroadcast.atlas.glycerin.model.Availability;
import com.metabroadcast.atlas.glycerin.model.AvailableVersions;
import com.metabroadcast.atlas.glycerin.model.PidReference;
import com.metabroadcast.atlas.glycerin.model.Programme;
import com.metabroadcast.atlas.glycerin.model.WarningTexts;
import com.metabroadcast.common.time.Clock;

/**
 * Base extractor for extracting common properties of {@link Item}s from a
 * {@link NitroItemSource}.
 *
 * @param <SOURCE> - the type of {@link Programme}.
 * @param <ITEM>   - the {@link Item} type extracted.
 */
public abstract class BaseNitroItemExtractor<SOURCE, ITEM extends Item>
        extends NitroContentExtractor<NitroItemSource<SOURCE>, ITEM> {

    private static final String AUDIO_DESCRIBED_VERSION_TYPE = "DubbedAudioDescribed";
    private static final String SIGNED_VERSION_TYPE = "Signed";
    private static final String WARNING_TEXT_LONG_LENGTH = "long";

    private final Predicate<WarningTexts.WarningText> IS_LONG_WARNING = new Predicate<WarningTexts.WarningText>() {
        @Override
        public boolean apply(WarningTexts.WarningText input) {
            return WARNING_TEXT_LONG_LENGTH.equals(input.getLength());
        }
    };

    private final NitroBroadcastExtractor broadcastExtractor
            = new NitroBroadcastExtractor();
    private final NitroAvailabilityExtractor availabilityExtractor
            = new NitroAvailabilityExtractor();

    public BaseNitroItemExtractor(Clock clock) {
        super(clock);
    }

    @Override
    protected final void extractAdditionalFields(NitroItemSource<SOURCE> source, ITEM item,
            DateTime now) {
        ImmutableSetMultimap<String, Broadcast> broadcasts = extractBroadcasts(source.getBroadcasts(),
                now);
        ImmutableSet.Builder<Version> versions = ImmutableSet.builder();

        for(AvailableVersions.Version nitroVersion : source.getAvailableVersions().getVersion()) {
            Set<Encoding> encodings = extractEncodings(nitroVersion, now, extractMediaType(source),
                    source.getProgrammeId());
            try {
                Version version = generateVersion(now, broadcasts, encodings, nitroVersion);
                versions.add(version);
            } catch (Exception e) {
                throw new RuntimeException("Exception processing version " + nitroVersion.getPid(), e);
            }
        }

        item.setVersions(versions.build());

        if (item instanceof Film) {
            item.setMediaType(MediaType.VIDEO);
            item.setSpecialization(Specialization.FILM);
        } else {
            extractMediaTypeAndSpecialization(source, item);
        }

        extractAdditionalItemFields(source, item, now);
    }


    private Version generateVersion(DateTime now,
            ImmutableSetMultimap<String, Broadcast> broadcasts,
            Set<Encoding> encodings,
            AvailableVersions.Version nitroVersion) {
        Version version = new Version();

        if (nitroVersion.getDuration() != null) {
            version.setDuration(convertDuration(nitroVersion.getDuration()));
        }

        version.setLastUpdated(now);
        version.setCanonicalUri(BbcFeeds.nitroUriForPid(nitroVersion.getPid()));
        version.setBroadcasts(broadcasts.get(nitroVersion.getPid()));

        setEncodingDetails(nitroVersion, encodings);
        version.setManifestedAs(encodings);

        Optional<WarningTexts.WarningText> warningText = warningTextFrom(nitroVersion);
        version.setRestriction(generateRestriction(warningText));
        return version;
    }

    private void setEncodingDetails(
            AvailableVersions.Version nitroVersion,
            Set<Encoding> encodings) {

        /**
         * Even if aspect ratio and the audio described and signed flags are on Version in the Nitro model,
         * in Atlas they naturally belong to Encoding
         */
        for (Encoding encoding : encodings) {
            //encoding.setVideoAspectRatio(nitroVersion.getAspectRatio());
            encoding.setAudioDescribed(isVersionOfType(nitroVersion, AUDIO_DESCRIBED_VERSION_TYPE));
            encoding.setSigned(isVersionOfType(nitroVersion, SIGNED_VERSION_TYPE));
        }
    }

    private Restriction generateRestriction(Optional<WarningTexts.WarningText> warningText) {
        Restriction restriction = new Restriction();
        restriction.setRestricted(false);

        if (warningText.isPresent()) {
            restriction.setRestricted(true);
            restriction.setMessage(warningText.get().getValue());
        }

        return restriction;
    }

    private boolean isVersionOfType(AvailableVersions.Version nitroVersion, String versionType) {
        List<AvailableVersions.Version.Types> versionTypes = nitroVersion.getTypes();

        if (versionTypes == null) {
            return false;
        }

        return Iterables.any(versionTypes, isOfType(versionType));
    }

    private Predicate<AvailableVersions.Version.Types> isOfType(final String versionType) {
        return new Predicate<AvailableVersions.Version.Types>() {
            @Override
            public boolean apply(
                    AvailableVersions.Version.Types input) {
                return input.getType().contains(versionType);
            }
        };
    }

    private void extractMediaTypeAndSpecialization(NitroItemSource<SOURCE> source, ITEM item) {
        String mediaType = extractMediaType(source);
        if (mediaType != null) {
            item.setMediaType(MediaType.fromKey(mediaType.toLowerCase()).orNull());
        }
        if (MediaType.VIDEO.equals(item.getMediaType())) {
            item.setSpecialization(Specialization.TV);
        } else if (MediaType.AUDIO.equals(item.getMediaType())) {
            item.setSpecialization(Specialization.RADIO);
        }
    }

    /**
     * Extract the media type of the source.
     *
     * @param source
     * @return the media type of the source, or null if not present.
     */
    protected abstract String extractMediaType(NitroItemSource<SOURCE> source);

    /**
     * Concrete implementations can override this method to perform additional
     * configuration of the extracted content from the source.
     *
     * @param source - the source data.
     * @param item   - the extracted item.
     * @param now    - the current time.
     */
    protected void extractAdditionalItemFields(NitroItemSource<SOURCE> source, ITEM item,
            DateTime now) {

    }

    private Set<Encoding> extractEncodings(AvailableVersions.Version version, DateTime now, String mediaType, String programmeId) {
        Set<Encoding> extracted = new HashSet<>();
        Iterator<AvailableVersions.Version.Availabilities> iterator = version.getAvailabilities().iterator();
        while (iterator.hasNext()) {
            extracted.addAll(availabilityExtractor.extract(
                    iterator.next().getAvailableVersionsAvailability(), mediaType, programmeId));
        }
        return setLastUpdated(extracted, now);
    }

    private Set<Encoding> setLastUpdated(Set<Encoding> encodings, DateTime now) {
        for (Encoding encoding : encodings) {
            encoding.setLastUpdated(now);
            for (Location location : encoding.getAvailableAt()) {
                location.setLastUpdated(now);
            }
        }

        return encodings;
    }

    private Map<String, Collection<Availability>> indexByVersion(
            List<Availability> availabilities) {
        ImmutableSetMultimap.Builder<String, Availability> availabilitiesByVersion
                = ImmutableSetMultimap.builder();
        for (Availability availability : availabilities) {
            String versionPid = checkNotNull(NitroUtil.versionPid(availability));
            availabilitiesByVersion.put(versionPid, availability);
        }
        return availabilitiesByVersion.build().asMap();
    }

    private ImmutableSetMultimap<String, Broadcast> extractBroadcasts(
            List<com.metabroadcast.atlas.glycerin.model.Broadcast> nitroBroadcasts, DateTime now) {
        Builder<String, Broadcast> broadcasts = ImmutableSetMultimap.builder();
        for (com.metabroadcast.atlas.glycerin.model.Broadcast broadcast : nitroBroadcasts) {
            Optional<Broadcast> extractedBroadcast = broadcastExtractor.extract(broadcast);
            if (extractedBroadcast.isPresent()) {
                broadcasts.put(versionPid(broadcast),
                        setLastUpdated(extractedBroadcast.get(), now));
            }
        }
        return broadcasts.build();
    }

    private Broadcast setLastUpdated(Broadcast broadcast, DateTime now) {
        broadcast.setLastUpdated(now);
        return broadcast;
    }

    private String versionPid(com.metabroadcast.atlas.glycerin.model.Broadcast broadcast) {
        for (PidReference pidRef : broadcast.getBroadcastOf()) {
            if ("version".equals(pidRef.getResultType())) {
                return pidRef.getPid();
            }
        }
        throw new IllegalArgumentException(String.format("No version ref for %s %s",
                broadcast.getClass().getSimpleName(), broadcast.getPid()));
    }

    private Duration convertDuration(javax.xml.datatype.Duration xmlDuration) {
        DateTime now = DateTime.now(DateTimeZone.UTC);
        return Duration.millis(xmlDuration.getTimeInMillis(now.toDate()));
    }

    private Optional<WarningTexts.WarningText> warningTextFrom(AvailableVersions.Version version) {
        AvailableVersions.Version.Warnings warnings = version.getWarnings();

        if (warnings == null) {
            return Optional.absent();
        }

        return Iterables.tryFind(warnings.getWarningTexts().getWarningText(), IS_LONG_WARNING);
    }

}

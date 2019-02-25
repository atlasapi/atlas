package org.atlasapi.remotesite.bbc.nitro.extract;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.Encoding;
import org.atlasapi.media.entity.Film;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Location;
import org.atlasapi.media.entity.MediaType;
import org.atlasapi.media.entity.Policy;
import org.atlasapi.media.entity.Restriction;
import org.atlasapi.media.entity.Specialization;
import org.atlasapi.media.entity.Version;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.remotesite.bbc.BbcFeeds;

import com.metabroadcast.atlas.glycerin.model.AvailableVersions;
import com.metabroadcast.atlas.glycerin.model.PidReference;
import com.metabroadcast.atlas.glycerin.model.Programme;
import com.metabroadcast.atlas.glycerin.model.WarningTexts;
import com.metabroadcast.common.time.Clock;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.ImmutableSetMultimap.Builder;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base extractor for extracting common properties of {@link Item}s from a
 * {@link NitroItemSource}.
 *
 * @param <SOURCE> - the type of {@link Programme}.
 * @param <ITEM>   - the {@link Item} type extracted.
 */
public abstract class BaseNitroItemExtractor<SOURCE, ITEM extends Item>
        extends NitroContentExtractor<NitroItemSource<SOURCE>, ITEM> {

    private static final Logger log = LoggerFactory.getLogger(BaseNitroItemExtractor.class);

    private static final String AUDIO_DESCRIBED_VERSION_TYPE = "DubbedAudioDescribed";
    private static final String SIGNED_VERSION_TYPE = "Signed";
    private static final String WARNING_TEXT_LONG_LENGTH = "long";

    private final NitroBroadcastExtractor broadcastExtractor
            = new NitroBroadcastExtractor();
    private final NitroAvailabilityExtractor availabilityExtractor
            = new NitroAvailabilityExtractor();

    public BaseNitroItemExtractor(Clock clock, ContentResolver contentResolver) {
        super(clock, contentResolver);
    }

    @Override
    protected final void extractAdditionalFields(
            NitroItemSource<SOURCE> source,
            ITEM item,
            Set<Version> existingVersions,
            DateTime now
    ) {
        ImmutableSetMultimap<String, Broadcast> broadcasts = extractBroadcasts(
                source.getBroadcasts(),
                now
        );

        Set<Version> versions = Sets.newConcurrentHashSet();

        AvailableVersions nitroVersions = extractVersions(source);
        if (nitroVersions != null) {

            // Versions without durations are basically invalid Nitro data. They aren't playable
            // and only cause issues down the line, so we don't ingest them. These usually get
            // fixed at some point by the beeb, so the next ingest job will pick them up.
            ImmutableList<AvailableVersions.Version> eligibleVersions =
                    FluentIterable.from(nitroVersions.getVersion())
                        .filter(input -> input.getDuration() != null)
                        .toList();
            for (AvailableVersions.Version nitroVersion : eligibleVersions) {

                String mediaType = extractMediaType(source);

                ImmutableSet.Builder<Encoding> encodingsBuilder = ImmutableSet.builder();

                for (AvailableVersions.Version.Availabilities availabilities : nitroVersion.getAvailabilities()) {
                    encodingsBuilder.addAll(availabilityExtractor.extractFromMixin(
                            extractPid(source),
                            availabilities.getAvailableVersionsAvailability(),
                            mediaType
                    ));
                }

                ImmutableSet<Encoding> encodings = encodingsBuilder.build();

                Version version = new Version();

                version.setDuration(convertDuration(nitroVersion.getDuration()));

                version.setLastUpdated(now);
                version.setCanonicalUri(BbcFeeds.nitroUriForPid(nitroVersion.getPid()));
                version.setBroadcasts(broadcasts.get(nitroVersion.getPid()));
                version.setManifestedAs(encodings);

                Optional<WarningTexts.WarningText> warningText = warningTextFrom(nitroVersion);
                version.setRestriction(generateRestriction(warningText));

                setEncodingDetails(nitroVersion, encodings);
                setLastUpdated(encodings, now);

                versions.add(version);
            }
        }

        item.setVersions(mergeVersions(existingVersions, versions));

        if (item instanceof Film) {
            item.setMediaType(MediaType.VIDEO);
            item.setSpecialization(Specialization.FILM);
        } else {
            extractMediaTypeAndSpecialization(source, item);
        }

        extractAdditionalItemFields(source, item, now);
    }

    private Set<Version> mergeVersions(
            Set<Version> existingVersions,
            Set<Version> ingestedVersions
    ) {
        Set<Version> newVersions = getNewVersions(existingVersions, ingestedVersions);

        for (Version existingVersion : existingVersions) {
            boolean versionFound = false;
            for (Version ingestedVersion : ingestedVersions) {
                // merge common versions
                if (existingVersion.getCanonicalUri().equals(ingestedVersion.getCanonicalUri())) {
                    versionFound = true;
                    existingVersion.setManifestedAs(mergeEncodings(
                            existingVersion.getManifestedAs(),
                            ingestedVersion.getManifestedAs()
                    ));
                    break;
                }
            }
            if (!versionFound) {
                // mark stale version as unavailable
                markEncodingsAsUnavailable(existingVersion.getManifestedAs());
            }
        }

        return Sets.union(existingVersions, newVersions);
    }
    
    private void markEncodingsAsUnavailable(Set<Encoding> encodings) {
        encodings.forEach(encoding -> {
            setCanonicalUriOn(encoding.getAvailableAt());
            markLocationsAsUnavailable(encoding.getAvailableAt());
            encoding.setAvailableAt(encoding.getAvailableAt()
                    .stream()
                    .filter(location -> !isLocationOld(location))
                    .collect(Collectors.toSet())
            );
        });
    }

    private Set<Version> getNewVersions(
            Set<Version> existingVersions,
            Set<Version> ingestedVersions
    ) {
        Set<String> canonicalUris = existingVersions.stream()
                .map(Identified::getCanonicalUri)
                .collect(Collectors.toSet());
        // get new versions
        return ingestedVersions.stream()
                .filter(version -> !canonicalUris.contains(version.getCanonicalUri()))
                .collect(Collectors.toSet());
    }

    private Set<Encoding> mergeEncodings(
            Set<Encoding> existingEncodings,
            Set<Encoding> ingestedEncodings
    ) {
        for (Encoding existingEncoding : existingEncodings) {
            boolean encodingFound = false;
            for (Encoding ingestedEncoding : ingestedEncodings) {
                // match the SD/HD encodings based on the videoBitRate
                if (existingEncoding.getVideoBitRate().equals(ingestedEncoding.getVideoBitRate())) {
                    encodingFound = true;
                    existingEncoding.setAvailableAt(mergeLocations(
                            existingEncoding.getAvailableAt(),
                            ingestedEncoding.getAvailableAt()
                    ));
                    break;
                }
            }
            if (!encodingFound) {
                // mark stale version as unavailable
                markEncodingsAsUnavailable(Sets.newHashSet(existingEncoding));
            }
        }

        return Sets.union(existingEncodings, ingestedEncodings);
    }

    private Set<Location> mergeLocations(
            Set<Location> existingLocations,
            Set<Location> ingestedLocations
    ) {
        // Some DB locations might not have the new canonical URI, so we update them 
        // in order to be able to compare them against the ones ingested from the API
        setCanonicalUriOn(existingLocations);
        Set<Location> staleLocations = filterStaleLocations(existingLocations, ingestedLocations);

        markLocationsAsUnavailable(staleLocations);

        //don't carry over locations that expired long time, so they get deleted
        Set<Location> recentStaleLocations = getRecentStaleLocations(staleLocations);

        return Sets.union(recentStaleLocations,ingestedLocations);
    }

    private Set<Location> getRecentStaleLocations(Set<Location> staleLocations) {
        return staleLocations.stream()
                .filter(location -> !isLocationOld(location))
                .collect(Collectors.toSet());
    }

    private Set<Location> filterStaleLocations(
            Set<Location> existingLocations,
            Set<Location> ingestedLocations
    ) {
        Set<String> ingestedUris = ingestedLocations.stream()
                .map(Identified::getCanonicalUri)
                .collect(Collectors.toSet());
        // Remove available locations that exist in both Nitro API and in DB. This should leave us
        // only with the DB locations not available in the Nitro API which should be marked as stale
        return existingLocations.stream()
                .filter(existingLocation -> !ingestedUris.contains(existingLocation.getCanonicalUri()))
                .collect(Collectors.toSet());
    }

    private void markLocationsAsUnavailable(Set<Location> staleLocations) {
        staleLocations.forEach(staleLocation -> {
            // Set location availability end if missing, so we can remove it completely after
            // 1 week in order to keep the DB clean
            if (Objects.isNull(staleLocation.getPolicy().getAvailabilityEnd())) {
                staleLocation.getPolicy().setAvailabilityEnd(DateTime.now().withTimeAtStartOfDay());
            }
            staleLocation.setAvailable(false);
            log.info(
                    "Marking location with canonical URI {} as unavailable",
                    staleLocation.getCanonicalUri()
            );
        });
    }

    private boolean isLocationOld(Location location) {
        Duration duration = new Duration(
                location.getPolicy().getAvailabilityEnd(),
                DateTime.now()
        );
        return duration.getStandardDays() > 7;
    }

    private void setCanonicalUriOn(Set<Location> existingLocations) {
        existingLocations.forEach(location -> {
            if (Strings.isNullOrEmpty(location.getCanonicalUri())) {
                location.setCanonicalUri(createLocationCanonicalUri(location));
            }
        });
    }

    private String createLocationCanonicalUri(Location location) {
        Policy policy = location.getPolicy();
        String networkKey = Objects.isNull(policy.getNetwork())
                            ? ""
                            : String.format("/%s", policy.getNetwork().key());
        return String.format(
                "%s/%s%s",
                location.getUri(),
                policy.getPlatform().key(),
                networkKey
        );
    }

    private void setEncodingDetails(
            AvailableVersions.Version nitroVersion,
            Set<Encoding> encodings) {

        /**
         * Even if aspect ratio and the audio described and signed flags are on Version in the Nitro model,
         * in Atlas they naturally belong to Encoding
         */
        for (Encoding encoding : encodings) {
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
        Iterable<String> types = Iterables.concat(Iterables.transform(
                versionTypes,
                new Function<AvailableVersions.Version.Types, List<String>>() {

                    @Override
                    public List<String> apply(AvailableVersions.Version.Types input) {
                        return input.getType();
                    }
                }
        ));

        return Iterables.any(types, isOfType(versionType));
    }

    private Predicate<String> isOfType(final String type) {
        return new Predicate<String>() {

            @Override
            public boolean apply(String input) {
                return type.equals(input);
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

    private Set<Encoding> setLastUpdated(Set<Encoding> encodings, DateTime now) {
        for (Encoding encoding : encodings) {
            encoding.setLastUpdated(now);
            for (Location location : encoding.getAvailableAt()) {
                location.setLastUpdated(now);
            }
        }

        return encodings;
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

        WarningTexts warningTexts = warnings.getWarningTexts();
        for (WarningTexts.WarningText warningText : warningTexts.getWarningText()) {
            if (WARNING_TEXT_LONG_LENGTH.equals(warningText.getLength())) {
                return Optional.of(warningText);
            }
        }

        return Optional.absent();
    }

}

package org.atlasapi.remotesite.bbc.nitro.extract;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;

import org.atlasapi.media.entity.Encoding;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Location;
import org.atlasapi.media.entity.Policy;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.people.QueuingPersonWriter;

import com.metabroadcast.atlas.glycerin.model.AvailableVersions;
import com.metabroadcast.atlas.glycerin.model.AvailableVersions.Version;
import com.metabroadcast.atlas.glycerin.model.AvailableVersions.Version.Availabilities;
import com.metabroadcast.atlas.glycerin.model.AvailableVersions.Version.Availabilities.Availability;
import com.metabroadcast.atlas.glycerin.model.AvailableVersions.Version.Availabilities.Availability.MediaSets.MediaSet;
import com.metabroadcast.atlas.glycerin.model.Episode;
import com.metabroadcast.common.time.SystemClock;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BaseNitroItemExtractorTest {

    private static final Duration VERSION_DURATION = Duration.standardMinutes(30);

    private static final int SD_VIDEO_BITRATE = 1_500_000;
    private static final int HD_VIDEO_BITRATE = 3_200_000;

    private static final String SD_MEDIA_SET = "iptv-all";
    private static final String HD_MEDIA_SET = "iptv-hd";

    private static final String ATLAS_VERSION_CANONICAL_URI = "http://nitro.bbc.co.uk/programmes/otherPID";
    private static final String ATLAS_LOCATION_CANONICAL_URI = "http://www.bbc.co.uk/iplayer/episode/otherPID/youview_iplayer";

    private static final String NITRO_VERSION_URI = "http://nitro.bbc.co.uk/programmes/b04gb78l";
    private static final String NITRO_LOCATION_URI = "http://www.bbc.co.uk/iplayer/episode/b04gb7b7/youview_iplayer";

    @Mock
    private ContentResolver contentResolver;
    @Mock
    private QueuingPersonWriter personWriter;

    private BaseNitroItemExtractor extractor;
    private DatatypeFactory dataTypeFactory;

    @Before
    public void setUp() throws DatatypeConfigurationException {
        extractor = new NitroEpisodeExtractor(new SystemClock(), contentResolver, personWriter);
        dataTypeFactory = DatatypeFactory.newInstance();
    }

    @Test
    public void testSdApiLocationIsAdded() {
        Episode episode = getNitroEpisode(Lists.newArrayList(getMediaSet(SD_MEDIA_SET)));
        NitroItemSource source = NitroItemSource.valueOf(episode);

        Item item = getAtlasItem();

        extractor.extractAdditionalFields(
                source,
                item,
                Sets.newConcurrentHashSet(),
                DateTime.now()
        );

        Encoding encoding = item.getVersions().iterator().next().getManifestedAs().iterator().next();
        assertEquals(SD_VIDEO_BITRATE, encoding.getVideoBitRate().intValue());
    }

    @Test
    public void testBothSdAndHdApiLocationsAreAdded() {
        Episode episode = getNitroEpisode(Lists.newArrayList(
                getMediaSet(SD_MEDIA_SET),
                getMediaSet(HD_MEDIA_SET)
        ));
        NitroItemSource source = NitroItemSource.valueOf(episode);

        Item item = getAtlasItem();

        extractor.extractAdditionalFields(
                source,
                item,
                Sets.newConcurrentHashSet(),
                DateTime.now()
        );

        Set<Encoding> encodings = item.getVersions().iterator().next().getManifestedAs();
        assertEquals(2, encodings.size());

        Iterator<Encoding> encodingsIterator = encodings.iterator();
        assertEquals(HD_VIDEO_BITRATE, encodingsIterator.next().getVideoBitRate().intValue());
        assertEquals(SD_VIDEO_BITRATE, encodingsIterator.next().getVideoBitRate().intValue());
    }

    @Test
    public void testStaleLocationIsMarkedUnavailableWhenLocationsDontMatch() {
        Episode episode = getNitroEpisode(Lists.newArrayList(getMediaSet(SD_MEDIA_SET)));
        NitroItemSource source = NitroItemSource.valueOf(episode);

        Item item = getAtlasItem();

        org.atlasapi.media.entity.Version atlasVersion = getAtlasVersion(
                NITRO_VERSION_URI,
                SD_VIDEO_BITRATE,
                ATLAS_LOCATION_CANONICAL_URI,
                DateTime.now().minusHours(2)
        );

        extractor.extractAdditionalFields(
                source,
                item,
                Sets.newHashSet(atlasVersion),
                DateTime.now()
        );

        // When the versions and the encodings match, compare the locations under the matching
        // encoding and mark the existing stale locations as unavailable
        Iterator<Location> locationIterator = item.getVersions()
                .iterator()
                .next()
                .getManifestedAs()
                .iterator()
                .next()
                .getAvailableAt()
                .iterator();
        Location extractedExistingLocation = locationIterator.next();
        assertFalse(extractedExistingLocation.getAvailable());

        Location extractedIngestedLocation = locationIterator.next();
        assertTrue(extractedIngestedLocation.getAvailable());
    }

    @Test
    public void testStaleLocationIsMarkedUnavailableWhenEncodingsDontMatch() {
        Episode episode = getNitroEpisode(Lists.newArrayList(getMediaSet(SD_MEDIA_SET)));
        NitroItemSource source = NitroItemSource.valueOf(episode);

        Item item = getAtlasItem();

        org.atlasapi.media.entity.Version atlasVersion = getAtlasVersion(
                NITRO_VERSION_URI,
                HD_VIDEO_BITRATE,
                ATLAS_LOCATION_CANONICAL_URI,
                new DateTime().withHourOfDay(1)
        );

        extractor.extractAdditionalFields(
                source,
                item,
                Sets.newHashSet(atlasVersion),
                DateTime.now()
        );

        // There could be only 2 types of encodings - SD and HD. When existing version's encodings
        // don't match with the ones from the API, mark all existing locations in the stale encoding
        // as unavailable
        Set<Encoding> extractedEncodings = item.getVersions().iterator().next().getManifestedAs();
        assertEquals(2, extractedEncodings.size());

        // Assert the existing encoding
        Iterator<Encoding> extractedEncodingIterator = extractedEncodings.iterator();
        Encoding extractedEncoding = extractedEncodingIterator.next();
        assertEquals(HD_VIDEO_BITRATE, extractedEncoding.getVideoBitRate().intValue());

        Set<Location> extractedLocations = extractedEncoding.getAvailableAt();
        assertEquals(1, extractedLocations.size());

        Location extractedLocation = extractedLocations.iterator().next();
        assertFalse(extractedLocation.getAvailable());
        assertEquals(ATLAS_LOCATION_CANONICAL_URI, extractedLocation.getCanonicalUri());

        // Assert the ingested encoding
        Encoding extractedIngestedEncoding = extractedEncodingIterator.next();
        assertEquals(1_500_000, extractedIngestedEncoding.getVideoBitRate().intValue());

        Set<Location> extractedIngestedLocations = extractedIngestedEncoding.getAvailableAt();
        assertEquals(1, extractedIngestedLocations.size());

        Location extractedIngestedLocation = extractedIngestedLocations.iterator().next();
        assertTrue(extractedIngestedLocation.getAvailable());
        assertEquals(
                "http://www.bbc.co.uk/iplayer/episode/b04gb7b7/youview_iplayer",
                extractedIngestedLocation.getCanonicalUri()
        );
    }

    @Test
    public void testStaleLocationIsMarkedUnavailableWhenVersionsDontMatch() {
        Episode episode = getNitroEpisode(Lists.newArrayList(getMediaSet(SD_MEDIA_SET)));
        NitroItemSource source = NitroItemSource.valueOf(episode);

        Item item = getAtlasItem();

        org.atlasapi.media.entity.Version atlasVersion = getAtlasVersion(
                ATLAS_VERSION_CANONICAL_URI,
                SD_VIDEO_BITRATE,
                ATLAS_LOCATION_CANONICAL_URI,
                new DateTime().withHourOfDay(1)
        );

        extractor.extractAdditionalFields(
                source,
                item,
                Sets.newHashSet(atlasVersion),
                DateTime.now()
        );

        Iterator<org.atlasapi.media.entity.Version> extractedVersionsIterator = item.getVersions().iterator();
        // Skip the new version from the API
        org.atlasapi.media.entity.Version extractedIngestedVersion = extractedVersionsIterator.next();

        // For existing versions that don't match with any version from the API, mark all existing
        // locations in each stale version as unavailable
        Location extractedLocation = extractedVersionsIterator.next()
                .getManifestedAs()
                .iterator()
                .next()
                .getAvailableAt()
                .iterator()
                .next();
        assertEquals(ATLAS_LOCATION_CANONICAL_URI, extractedLocation.getCanonicalUri());
        assertFalse(extractedLocation.getAvailable());
    }

    @Test
    public void testExistingLocationIsOverriddenByMatchingIngestedLocation() {
        Episode episode = getNitroEpisode(Lists.newArrayList(getMediaSet("iptv-all")));
        NitroItemSource source = NitroItemSource.valueOf(episode);

        Item item = getAtlasItem();

        org.atlasapi.media.entity.Version atlasVersion = getAtlasVersion(
                ATLAS_VERSION_CANONICAL_URI,
                SD_VIDEO_BITRATE,
                NITRO_LOCATION_URI,
                new DateTime().withHourOfDay(1)
        );

        extractor.extractAdditionalFields(
                source,
                item,
                Sets.newHashSet(atlasVersion),
                DateTime.now()
        );

        // When existing location matches a location from the API, the ingested location overrides
        // the existing location
        Location extractedLocation = item.getVersions()
                .iterator()
                .next()
                .getManifestedAs()
                .iterator()
                .next()
                .getAvailableAt()
                .iterator()
                .next();
        assertEquals(NITRO_LOCATION_URI, extractedLocation.getCanonicalUri());
        assertTrue(extractedLocation.getAvailable());
        assertEquals(
                new DateTime().withHourOfDay(2).getHourOfDay(),
                extractedLocation.getPolicy().getAvailabilityStart().getHourOfDay()
        );
    }

    @Test
    public void testExistingLocationIsMarkedUnavailableWhenNoVersionsAvailableInTheApi() {
        Episode episode = new Episode();
        episode.setPid("b04gb7b7");
        episode.setTitle("River Lagan");

        NitroItemSource source = NitroItemSource.valueOf(episode);

        Item item = getAtlasItem();

        org.atlasapi.media.entity.Version atlasVersion = getAtlasVersion(
                ATLAS_VERSION_CANONICAL_URI,
                SD_VIDEO_BITRATE,
                ATLAS_LOCATION_CANONICAL_URI,
                new DateTime().withHourOfDay(1)
        );

        extractor.extractAdditionalFields(
                source,
                item,
                Sets.newHashSet(atlasVersion),
                DateTime.now()
        );

        // If no version is available in Nitro API, mark all existing locations as unavailable
        Location extractedLocation = item.getVersions()
                .iterator()
                .next()
                .getManifestedAs()
                .iterator()
                .next()
                .getAvailableAt()
                .iterator()
                .next();
        assertEquals(ATLAS_LOCATION_CANONICAL_URI, extractedLocation.getCanonicalUri());
        assertFalse(extractedLocation.getAvailable());
    }

    @Test
    public void testExistingLocationIsRemovedIfOlderThan7Days() {
        Episode episode = new Episode();
        episode.setPid("b04gb7b7");
        NitroItemSource nitroItemSource = NitroItemSource.valueOf(episode);

        Item item = getAtlasItem();

        org.atlasapi.media.entity.Version atlasVersion = getAtlasVersion(
                ATLAS_VERSION_CANONICAL_URI,
                SD_VIDEO_BITRATE,
                ATLAS_LOCATION_CANONICAL_URI,
                new DateTime().withHourOfDay(1).minusDays(10)
        );

        extractor.extractAdditionalFields(
                nitroItemSource,
                item,
                Sets.newHashSet(atlasVersion),
                DateTime.now()
        );

        // If existing location has the availability end > 7 days in the past, it should be removed
        Set<Location> extractedLocations = item.getVersions()
                .iterator()
                .next()
                .getManifestedAs()
                .iterator()
                .next()
                .getAvailableAt();
        assertEquals(0, extractedLocations.size());
    }

    private MediaSet getMediaSet(String mediaSetName) {
        MediaSet sdMediaSet = new MediaSet();
        sdMediaSet.setName(mediaSetName);
        DateTime sdActualStart = new DateTime(2019, 2, 13, 6, 47, 54)
                .withZone(DateTimeZone.UTC);
        XMLGregorianCalendar sdXmlActualStart = dataTypeFactory.newXMLGregorianCalendar(
                sdActualStart.toGregorianCalendar()
        );
        sdMediaSet.setActualStart(sdXmlActualStart);
        return sdMediaSet;
    }

    private Episode getNitroEpisode(List<MediaSet> mediaSets) {
        Episode episode = new Episode();
        episode.setPid("b04gb7b7");
        episode.setTitle("River Lagan");
        episode.setMediaType("Video");
        episode.setAvailableVersions(getNitroAvailableVersion(mediaSets));
        return episode;
    }

    private Item getAtlasItem() {
        Item item = new Item();
        item.setCanonicalUri("http://nitro.bbc.co.uk/programmes/b04gb7b7");
        return item;
    }

    private org.atlasapi.media.entity.Version getAtlasVersion(
            String versionCanonicalUri,
            int encodingBitRate,
            String locationCanonicalUri,
            DateTime locationAvailabilityEnd
    ) {
        org.atlasapi.media.entity.Version atlasVersion = new org.atlasapi.media.entity.Version();
        atlasVersion.setCanonicalUri(versionCanonicalUri);

        Encoding atlasEncoding = new Encoding();
        atlasEncoding.setVideoBitRate(encodingBitRate);
        atlasVersion.setManifestedAs(Sets.newHashSet(atlasEncoding));

        Location atlasLocation = new Location();
        atlasLocation.setCanonicalUri(locationCanonicalUri);

        Policy policy = new Policy();
        policy.setAvailabilityEnd(locationAvailabilityEnd);
        atlasLocation.setPolicy(policy);
        atlasEncoding.setAvailableAt(Sets.newHashSet(atlasLocation));
        return atlasVersion;
    }

    private AvailableVersions getNitroAvailableVersion(List<MediaSet> nitroMediaSets) {
        Availability availability = new Availability();
        availability.setType("ondemand");
        DateTime scheduleStart = new DateTime().withHourOfDay(2)
                .withZone(DateTimeZone.UTC);
        XMLGregorianCalendar xmlScheduleStart = dataTypeFactory.newXMLGregorianCalendar(
                scheduleStart.toGregorianCalendar()
        );
        availability.setScheduledStart(xmlScheduleStart);
        DateTime scheduleEnd = new DateTime().withHourOfDay(5)
                .withZone(DateTimeZone.UTC);
        XMLGregorianCalendar xmlScheduleEnd = dataTypeFactory.newXMLGregorianCalendar(
                scheduleEnd.toGregorianCalendar()
        );
        availability.setScheduledEnd(xmlScheduleEnd);

        Availability.MediaSets mediaSets = new Availability.MediaSets();
        mediaSets.getMediaSet().addAll(nitroMediaSets);
        availability.setMediaSets(mediaSets);

        Availabilities availabilities = new Availabilities();
        availabilities.getAvailableVersionsAvailability().add(availability);

        Version version = new Version();
        version.getAvailabilities().add(availabilities);
        version.setDuration(dataTypeFactory.newDuration(VERSION_DURATION.getMillis()));
        version.setPid("b04gb78l");

        AvailableVersions availableVersion = new AvailableVersions();
        availableVersion.getVersion().add(version);

        return availableVersion;
    }

}
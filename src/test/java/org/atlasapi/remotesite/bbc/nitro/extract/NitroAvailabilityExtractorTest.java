package org.atlasapi.remotesite.bbc.nitro.extract;

import java.util.Collections;
import java.util.Set;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;

import org.atlasapi.media.TransportType;
import org.atlasapi.media.entity.Encoding;
import org.atlasapi.media.entity.Location;
import org.atlasapi.media.entity.Policy;

import com.metabroadcast.atlas.glycerin.model.Availability;
import com.metabroadcast.atlas.glycerin.model.AvailabilityOf;
import com.metabroadcast.atlas.glycerin.model.AvailableVersions.Version.Availabilities;
import com.metabroadcast.atlas.glycerin.model.AvailableVersions.Version.Availabilities.Availability.MediaSets;
import com.metabroadcast.atlas.glycerin.model.AvailableVersions.Version.Availabilities.Availability.MediaSets.MediaSet;
import com.metabroadcast.common.intl.Countries;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class NitroAvailabilityExtractorTest {

    private static final String VERSION_PID = "p02ccx7g";
    private static final String EPISODE_PID = "p02ccx9g";
    private static final String VIDEO_MEDIA_TYPE = "Video";
    private static final String AUDIO_MEDIA_TYPE = "Audio";
    
    private final NitroAvailabilityExtractor extractor = new NitroAvailabilityExtractor();
    private DatatypeFactory dataTypeFactory;

    @Before
    public void setUp() throws DatatypeConfigurationException {
        dataTypeFactory = DatatypeFactory.newInstance();
    }
    
    @Test
    public void testAvailabilityExtraction() {
        Availability hdAvailability = hdAvailability(EPISODE_PID, VERSION_PID);
        Availability sdAvailability = sdAvailability(EPISODE_PID, VERSION_PID);

        Encoding hdEncoding = Iterables.getOnlyElement(extractor.extract(ImmutableList.of(hdAvailability), VIDEO_MEDIA_TYPE));
        Encoding sdEncoding = Iterables.getOnlyElement(extractor.extract(ImmutableList.of(sdAvailability), VIDEO_MEDIA_TYPE));

        assertEquals(3200000, (int) hdEncoding.getVideoBitRate());
        assertEquals(1280, (int) hdEncoding.getVideoHorizontalSize());
        assertEquals(720, (int) hdEncoding.getVideoVerticalSize());

        assertEquals(1500000, (int) sdEncoding.getVideoBitRate());
        assertEquals(640, (int) sdEncoding.getVideoHorizontalSize());
        assertEquals(360, (int) sdEncoding.getVideoVerticalSize());
        assertFalse(sdEncoding.getSubtitled());
    }
    
    @Test
    public void testSubtitledFlagExtraction() {
        Set<Availability> availabilities = ImmutableSet.of(sdAvailability(EPISODE_PID, VERSION_PID), captionsAvailability(null, null));
        Encoding encoding = Iterables.getOnlyElement(extractor.extract(availabilities, VIDEO_MEDIA_TYPE));
        assertTrue(encoding.getSubtitled());
    }

    @Test
    public void testRadioActualAvailabilityExtraction() throws DatatypeConfigurationException {
        Availability availability = hdAvailability(EPISODE_PID, VERSION_PID);
        availability.setStatus("available");
        DateTime actualStart = new DateTime().withZone(DateTimeZone.UTC);
        XMLGregorianCalendar xmlStart = DatatypeFactory
                .newInstance().newXMLGregorianCalendar(actualStart.toGregorianCalendar());
        availability.setActualStart(xmlStart);
        availability.setStatus("available");
        
        Location locationActuallyAvailable = getOnlyLocationFrom(extractor.extract(ImmutableSet.of(availability), AUDIO_MEDIA_TYPE));
        assertEquals(actualStart, locationActuallyAvailable.getPolicy().getActualAvailabilityStart());
        
        availability.setStatus("unavailable");
        Location locationNotActuallyAvailable = getOnlyLocationFrom(extractor.extract(ImmutableSet.of(availability), AUDIO_MEDIA_TYPE));
        assertNull(locationNotActuallyAvailable.getPolicy().getActualAvailabilityStart());
    }
    
    @Test
    public void testVideoActualAvailabilityExtraction() {
        Availability availability = hdAvailability(EPISODE_PID, VERSION_PID);
        availability.setStatus("available");
        DateTime actualStart = new DateTime().withZone(DateTimeZone.UTC);
        XMLGregorianCalendar xmlStart = dataTypeFactory.newXMLGregorianCalendar(actualStart.toGregorianCalendar());
        availability.setActualStart(xmlStart);
        availability.setStatus("unavailable");
        
        Location locationActuallyAvailable = getOnlyLocationFrom(extractor.extract(ImmutableSet.of(availability), VIDEO_MEDIA_TYPE));
        assertEquals(actualStart, locationActuallyAvailable.getPolicy().getActualAvailabilityStart());
        
        availability.setStatus("unavailable");
        Location locationNotActuallyAvailable = getOnlyLocationFrom(extractor.extract(ImmutableSet.of(availability), VIDEO_MEDIA_TYPE));
        assertEquals(actualStart, locationNotActuallyAvailable.getPolicy().getActualAvailabilityStart());
    }

    @Test
    public void testDontIngestActualAvailabilityStartInTheFuture() {
        Availability availability = hdAvailability(EPISODE_PID, VERSION_PID);
        DateTime actualStart = new DateTime().withZone(DateTimeZone.UTC).plusDays(1);
        XMLGregorianCalendar xmlStart = dataTypeFactory.newXMLGregorianCalendar(actualStart.toGregorianCalendar());
        availability.setActualStart(xmlStart);
        availability.setStatus("unavailable");
        
        Location location = getOnlyLocationFrom(extractor.extract(ImmutableSet.of(availability), VIDEO_MEDIA_TYPE));
        assertNull(location.getPolicy().getActualAvailabilityStart());
    }
    
    @Test
    public void testDontIngestActualAvailabilityStartForRevokedAvailability() {
        Availability availability = hdAvailability(EPISODE_PID, VERSION_PID);
        
        DateTime actualStart = new DateTime().withZone(DateTimeZone.UTC).minusDays(1);
        XMLGregorianCalendar xmlStart = dataTypeFactory.newXMLGregorianCalendar(actualStart.toGregorianCalendar());
        availability.setActualStart(xmlStart);
        
        availability.setRevocationStatus("revoked");
        
        Location location = getOnlyLocationFrom(extractor.extract(ImmutableSet.of(availability), VIDEO_MEDIA_TYPE));
        assertNull(location.getPolicy().getActualAvailabilityStart());
    }
    
    private Location getOnlyLocationFrom(Set<Encoding> encodings) {
        return Iterables.getOnlyElement(Iterables.getOnlyElement(encodings).getAvailableAt());
        
    }

    @Test
    public void testLocationExistsInBothApiAndDatabase() throws DatatypeConfigurationException {
        Availabilities.Availability availability = getAvailability();
        DateTime actualStart = DateTime.parse("2019-01-01").withZone(DateTimeZone.UTC);
        XMLGregorianCalendar xmlStart = DatatypeFactory
                .newInstance().newXMLGregorianCalendar(actualStart.toGregorianCalendar());
        availability.setScheduledStart(xmlStart);
        Location existingLocation = baseExistingLocation();

        Set<Encoding> encodings = extractor.extractFromMixin(
                "pid",
                Collections.singleton(availability),
                VIDEO_MEDIA_TYPE
        );

        Location extractedLocation = getOnlyLocationFrom(encodings);
        assertEquals(extractedLocation.getUri(), existingLocation.getUri());
        assertTrue(extractedLocation.getAvailable());
    }

    @Test
    public void testNewLocationIsAdded() throws DatatypeConfigurationException {
        Availabilities.Availability availability = getAvailability();

        Set<Encoding> encodings = extractor.extractFromMixin(
                "pid",
                Collections.singleton(availability),
                VIDEO_MEDIA_TYPE
        );

        Location extractedLocation = getOnlyLocationFrom(encodings);
        assertEquals(extractedLocation.getUri(), "http://www.bbc.co.uk/iplayer/episode/pid");
        assertEquals(extractedLocation.getTransportType(), TransportType.LINK);
        assertTrue(extractedLocation.getAvailable());

        Policy extractedPolicy = extractedLocation.getPolicy();
        assertEquals(extractedPolicy.getPlatform(), Policy.Platform.YOUVIEW_IPLAYER);
        assertEquals(extractedPolicy.getAvailableCountries(), ImmutableSet.of(Countries.GB));
    }

    @Test @Ignore
    public void testExistingLocationIsMarkedUnavailable() {
        Availabilities.Availability availability = getAvailability();
        Location existingLocation = baseExistingLocation();
        existingLocation.setUri("differentUri");
        existingLocation.getPolicy().setAvailabilityEnd(DateTime.parse("2019-01-29"));

        Set<Encoding> encodings = extractor.extractFromMixin(
                "pid",
                Collections.singleton(availability),
                VIDEO_MEDIA_TYPE
        );

        Set<Location> extractedLocations = encodings.iterator().next().getAvailableAt();
        assertEquals(extractedLocations.size(), 2);

        extractedLocations.forEach(extractedLocation -> {
            if (extractedLocation.getUri().equals("differentUri")) {
                assertFalse(extractedLocation.getAvailable());
            } else {
                assertTrue(extractedLocation.getAvailable());
            }
        });
    }

    private Availabilities.Availability getAvailability() {
        MediaSet mediaSet = new MediaSet();
        mediaSet.setName("iptv-all");

        MediaSets mediaSets = new MediaSets();
        mediaSets.getMediaSet().add(mediaSet);

        Availabilities.Availability availability = new Availabilities.Availability();
        availability.setMediaSets(mediaSets);

        return availability;
    }

    private Location baseExistingLocation() {
        Location existingLocation = new Location();
        existingLocation.setUri("http://www.bbc.co.uk/iplayer/episode/pid");
        existingLocation.setTransportType(TransportType.LINK);
        existingLocation.setPolicy(getBasePolicy());
        existingLocation.setAvailable(true);
        return existingLocation;
    }

    private Policy getBasePolicy() {
        Policy policy = new Policy();
        policy.setAvailabilityStart(DateTime.parse("2019-01-01"));
        policy.setPlatform(Policy.Platform.YOUVIEW_IPLAYER);
        policy.setAvailableCountries(ImmutableSet.of(Countries.GB));
        return policy;
    }

    private Availability baseAvailability(String episodePid, String versionPid) {
        Availability availability = new Availability();
        
        AvailabilityOf availabilityOfVersion = new AvailabilityOf();
        availabilityOfVersion.setPid(versionPid);
        availabilityOfVersion.setResultType("version");
        availability.getAvailabilityOf().add(availabilityOfVersion);

        AvailabilityOf availabilityOfEpisode = new AvailabilityOf();
        availabilityOfEpisode.setPid(episodePid);
        availabilityOfEpisode.setResultType("episode");
        availability.getAvailabilityOf().add(availabilityOfEpisode);
        return availability;
    }

    private Availability hdAvailability(String episodePid, String versionPid) {
        Availability availability = baseAvailability(episodePid, versionPid);
        availability.getMediaSet().add("iptv-hd");
        availability.getMediaSet().add("iptv-all");

        return availability;
    }

    private Availability sdAvailability(String episodePid, String versionPid) {
        Availability availability = baseAvailability(episodePid, versionPid);
        availability.getMediaSet().add("iptv-all");
        availability.getMediaSet().add("iptv-sd");

        return availability;
    }
    
    
    private Availability captionsAvailability(String episodePid, String versionPid) {
        Availability availability = baseAvailability(episodePid, versionPid);
        availability.getMediaSet().add("captions");
        availability.setStatus("available");
        return availability;
    }

}

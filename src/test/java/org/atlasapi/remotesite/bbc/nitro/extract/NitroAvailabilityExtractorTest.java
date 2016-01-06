package org.atlasapi.remotesite.bbc.nitro.extract;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Set;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;

import org.atlasapi.media.entity.Encoding;
import org.atlasapi.media.entity.Location;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.metabroadcast.atlas.glycerin.model.AvailableVersions.Version.Availabilities.Availability;

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
        Availability hdAvailability = hdAvailability();
        Availability sdAvailability = sdAvailability();

        Encoding hdEncoding = Iterables.getOnlyElement(extractor.extract(ImmutableList.of(hdAvailability), VIDEO_MEDIA_TYPE, EPISODE_PID));
        Encoding sdEncoding = Iterables.getOnlyElement(extractor.extract(ImmutableList.of(sdAvailability), VIDEO_MEDIA_TYPE, EPISODE_PID));

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
        Set<Availability> availabilities = ImmutableSet.of(sdAvailability(), captionsAvailability());
        Encoding encoding = Iterables.getOnlyElement(extractor.extract(availabilities, VIDEO_MEDIA_TYPE, EPISODE_PID));
        assertTrue(encoding.getSubtitled());
    }

    @Test
    public void testRadioActualAvailabilityExtraction() throws DatatypeConfigurationException {

        DateTime actualStart = new DateTime().withZone(DateTimeZone.UTC);
        XMLGregorianCalendar xmlStart = DatatypeFactory
                .newInstance().newXMLGregorianCalendar(actualStart.toGregorianCalendar());
        Availability availability = getAvailabilityWithActualStart(xmlStart);
        availability.setStatus("available");
        
        Location locationActuallyAvailable = getOnlyLocationFrom(extractor.extract(ImmutableSet.of(availability), AUDIO_MEDIA_TYPE, EPISODE_PID));
        assertEquals(actualStart, locationActuallyAvailable.getPolicy().getActualAvailabilityStart());
        
        availability.setStatus("unavailable");
        Location locationNotActuallyAvailable = getOnlyLocationFrom(extractor.extract(ImmutableSet.of(availability), AUDIO_MEDIA_TYPE, EPISODE_PID));
        assertNull(locationNotActuallyAvailable.getPolicy().getActualAvailabilityStart());
    }
    
    @Test
    public void testVideoActualAvailabilityExtraction() {
        DateTime actualStart = new DateTime().withZone(DateTimeZone.UTC);
        XMLGregorianCalendar xmlStart = dataTypeFactory.newXMLGregorianCalendar(actualStart.toGregorianCalendar());
        Availability availability = getAvailabilityWithActualStart(xmlStart);
        availability.setStatus("unavailable");
        
        Location locationActuallyAvailable = getOnlyLocationFrom(extractor.extract(ImmutableSet.of(availability), VIDEO_MEDIA_TYPE, EPISODE_PID));
        assertEquals(actualStart, locationActuallyAvailable.getPolicy().getActualAvailabilityStart());
        
        availability.setStatus("unavailable");
        Location locationNotActuallyAvailable = getOnlyLocationFrom(extractor.extract(ImmutableSet.of(availability), VIDEO_MEDIA_TYPE, EPISODE_PID));
        assertEquals(actualStart, locationNotActuallyAvailable.getPolicy().getActualAvailabilityStart());
    }

    @Test
    public void testDontIngestActualAvailabilityStartInTheFuture() {
        DateTime actualStart = new DateTime().withZone(DateTimeZone.UTC).plusDays(1);
        XMLGregorianCalendar xmlStart = dataTypeFactory.newXMLGregorianCalendar(actualStart.toGregorianCalendar());
        Availability availability = getAvailabilityWithActualStart(xmlStart);
        availability.setStatus("unavailable");
        
        Location location = getOnlyLocationFrom(extractor.extract(ImmutableSet.of(availability), VIDEO_MEDIA_TYPE, EPISODE_PID));
        assertNull(location.getPolicy().getActualAvailabilityStart());
    }
    
    @Test @Ignore
    public void testDontIngestActualAvailabilityStartForRevokedAvailability() {

        DateTime actualStart = new DateTime().withZone(DateTimeZone.UTC).minusDays(1);
        XMLGregorianCalendar xmlStart = dataTypeFactory.newXMLGregorianCalendar(actualStart.toGregorianCalendar());
        Availability availability = getAvailabilityWithActualStart(xmlStart);
        //availability.setRevocationStatus("revoked");
        
        Location location = getOnlyLocationFrom(extractor.extract(ImmutableSet.of(availability), VIDEO_MEDIA_TYPE, EPISODE_PID));
        assertNull(location.getPolicy().getActualAvailabilityStart());
    }
    
    private Location getOnlyLocationFrom(Set<Encoding> encodings) {
        return Iterables.getOnlyElement(Iterables.getOnlyElement(encodings).getAvailableAt());
        
    }

    private Availability sdAvailability() {

        Availability availability = new Availability();
        Availability.MediaSets mediaSets = new Availability.MediaSets();

        Availability.MediaSets.MediaSet sdMediaSet = new Availability.MediaSets.MediaSet();
        sdMediaSet.setName("iptv-sd");
        Availability.MediaSets.MediaSet allMediaSet = new Availability.MediaSets.MediaSet();
        allMediaSet.setName("iptv-all");

        mediaSets.getMediaSet().add(sdMediaSet);
        mediaSets.getMediaSet().add(allMediaSet);

        availability.setMediaSets(mediaSets);

        return availability;
    }

    private Availability hdAvailability() {
        Availability.MediaSets mediaSets = new Availability.MediaSets();
        Availability availability = new Availability();
        Availability.MediaSets.MediaSet hdMediaSet = new Availability.MediaSets.MediaSet();
        hdMediaSet.setName("iptv-hd");
        Availability.MediaSets.MediaSet allMediaSet = new Availability.MediaSets.MediaSet();
        allMediaSet.setName("iptv-all");
        mediaSets.getMediaSet().add(hdMediaSet);
        mediaSets.getMediaSet().add(allMediaSet);
        availability.setMediaSets(mediaSets);
        return availability;
    }

    private Availability captionsAvailability() {
        Availability.MediaSets mediaSets = new Availability.MediaSets();
        Availability availability = new Availability();
        Availability.MediaSets.MediaSet mediaSet = new Availability.MediaSets.MediaSet();
        mediaSet.setName("captions");
        availability.setStatus("available");
        mediaSets.getMediaSet().add(mediaSet);
        availability.setMediaSets(mediaSets);

        return availability;
    }

    private Availability getAvailabilityWithActualStart(XMLGregorianCalendar start) {
        Availability availability = new Availability();
        Availability.MediaSets mediaSets = new Availability.MediaSets();
        Availability.MediaSets.MediaSet sdMediaSet = new Availability.MediaSets.MediaSet();
        sdMediaSet.setName("iptv-sd");
        sdMediaSet.setActualStart(start);

        Availability.MediaSets.MediaSet allMediaSet = new Availability.MediaSets.MediaSet();
        allMediaSet.setName("iptv-all");
        allMediaSet.setActualStart(start);

        mediaSets.getMediaSet().add(sdMediaSet);
        mediaSets.getMediaSet().add(allMediaSet);
        availability.setMediaSets(mediaSets);
        return availability;

    }

}

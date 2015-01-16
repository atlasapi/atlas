package org.atlasapi.remotesite.bbc.nitro.extract;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.Set;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;

import org.atlasapi.media.entity.Encoding;
import org.atlasapi.media.entity.Location;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.metabroadcast.atlas.glycerin.model.Availability;
import com.metabroadcast.atlas.glycerin.model.AvailabilityOf;

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
        availability.setStatus("available");
        DateTime actualStart = new DateTime().withZone(DateTimeZone.UTC).plusDays(1);
        XMLGregorianCalendar xmlStart = dataTypeFactory.newXMLGregorianCalendar(actualStart.toGregorianCalendar());
        availability.setActualStart(xmlStart);
        availability.setStatus("unavailable");
        
        Location location = getOnlyLocationFrom(extractor.extract(ImmutableSet.of(availability), VIDEO_MEDIA_TYPE));
        assertNull(location.getPolicy().getActualAvailabilityStart());
    }
    private Location getOnlyLocationFrom(Set<Encoding> encodings) {
        return Iterables.getOnlyElement(Iterables.getOnlyElement(encodings).getAvailableAt());
        
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

}

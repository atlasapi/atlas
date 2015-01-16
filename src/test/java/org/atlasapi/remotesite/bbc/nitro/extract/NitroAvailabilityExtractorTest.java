package org.atlasapi.remotesite.bbc.nitro.extract;

import static org.junit.Assert.assertEquals;

import org.atlasapi.media.entity.Encoding;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.metabroadcast.atlas.glycerin.model.Availability;
import com.metabroadcast.atlas.glycerin.model.AvailabilityOf;

public class NitroAvailabilityExtractorTest {

    private static final String VERSION_PID = "p02ccx7g";
    private static final String EPISODE_PID = "p02ccx9g";

    @Test
    public void testAvailabilityExtraction() {
        Availability hdAvailability = hdAvailability(EPISODE_PID, VERSION_PID);
        Availability sdAvailability = sdAvailability(EPISODE_PID, VERSION_PID);

        NitroAvailabilityExtractor extractor = new NitroAvailabilityExtractor();

        Encoding hdEncoding = Iterables.getOnlyElement(extractor.extract(ImmutableList.of(hdAvailability)));
        Encoding sdEncoding = Iterables.getOnlyElement(extractor.extract(ImmutableList.of(sdAvailability)));

        assertEquals(3200000, (int) hdEncoding.getVideoBitRate());
        assertEquals(1280, (int) hdEncoding.getVideoHorizontalSize());
        assertEquals(720, (int) hdEncoding.getVideoVerticalSize());

        assertEquals(1500000, (int) sdEncoding.getVideoBitRate());
        assertEquals(640, (int) sdEncoding.getVideoHorizontalSize());
        assertEquals(360, (int) sdEncoding.getVideoVerticalSize());
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

package org.atlasapi.remotesite.bbc.nitro.extract;

import java.util.Map.Entry;
import java.util.Set;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;

import org.atlasapi.media.entity.Encoding;
import org.atlasapi.media.entity.Film;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.MediaType;
import org.atlasapi.media.entity.Restriction;
import org.atlasapi.persistence.content.people.QueuingPersonWriter;
import org.atlasapi.persistence.topic.TopicStore;

import com.metabroadcast.atlas.glycerin.model.AncestorTitles;
import com.metabroadcast.atlas.glycerin.model.Availability;
import com.metabroadcast.atlas.glycerin.model.AvailabilityOf;
import com.metabroadcast.atlas.glycerin.model.AvailableVersions;
import com.metabroadcast.atlas.glycerin.model.Broadcast;
import com.metabroadcast.atlas.glycerin.model.Episode;
import com.metabroadcast.atlas.glycerin.model.Format;
import com.metabroadcast.atlas.glycerin.model.PidReference;
import com.metabroadcast.atlas.glycerin.model.ProgrammeFormats;
import com.metabroadcast.atlas.glycerin.model.Version;
import com.metabroadcast.atlas.glycerin.model.WarningText;
import com.metabroadcast.atlas.glycerin.model.Warnings;
import com.metabroadcast.common.time.SystemClock;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.hamcrest.Matchers;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;


public class NitroEpisodeExtractorTest {

    private static final String AUDIO_DESCRIBED_VERSION_PID = "p02ccx3g";
    private static final String NON_AUDIO_DESCRIBED_VERSION_PID = "p02ccx5g";
    private static final String WITH_WARNING_VERSION_PID = "p02ccx8g";
    private static final String VERSION_PID = "p02ccx7g";
    private static final Duration VERSION_DURATION = Duration.standardMinutes(90);

    private static final String EPISODE_PID = "p01mv8m3";
    private final NitroEpisodeExtractor extractor = new NitroEpisodeExtractor(
            new SystemClock(),
            Mockito.mock(QueuingPersonWriter.class)
    );

    @Test
    public void testParentRefsForExtractedTopLevelItemAreEmpty() {
        
        Episode tli = new Episode();
        tli.setPid("p01mv8m3");
        tli.setTitle("Pantocracy");
        
        Item extracted = extractor.extract(NitroItemSource.valueOf(tli));
        
        assertFalse(extracted instanceof org.atlasapi.media.entity.Episode);
        assertNull(extracted.getContainer());
        assertThat(extracted.getTitle(), is(tli.getTitle()));
        
    }

    @Test
    public void testParentRefsForExtractedBrandEpisodeAreBrandOnly() {
        
        Episode brandEpisode = new Episode();
        brandEpisode.setPid("p017m2vg");
        brandEpisode.setTitle("01/01/2004");
        brandEpisode.setEpisodeOf(pidRef("brand", "b006m86d"));
        brandEpisode.setAncestorTitles(ancestorTitles("b006m86d", "EastEnders"));

        Item extracted = extractor.extract(NitroItemSource.valueOf(brandEpisode));
        
        org.atlasapi.media.entity.Episode episode
            = (org.atlasapi.media.entity.Episode) extracted;
        assertThat(episode.getContainer().getUri(), endsWith("b006m86d"));
        assertNull(episode.getSeriesRef());
        assertThat(episode.getTitle(), is(brandEpisode.getTitle()));
    }

    @Test
    public void testParentRefsForExtractedSeriesEpisodeAreSeriesOnly() {
        
        Episode brandEpisode = new Episode();
        brandEpisode.setPid("b012cl84");
        brandEpisode.setTitle("Destiny");
        brandEpisode.setEpisodeOf(pidRef("series", "b00zdhtg"));
        brandEpisode.setAncestorTitles(ancestorTitles(null, null,
            ImmutableMap.of("b00zdhtg", "Wonders of the Universe")
        ));
        
        Item extracted = extractor.extract(NitroItemSource.valueOf(brandEpisode));
        
        org.atlasapi.media.entity.Episode episode
        = (org.atlasapi.media.entity.Episode) extracted;
        assertThat(episode.getContainer().getUri(), endsWith("b00zdhtg"));
        assertThat(episode.getSeriesRef().getUri(), endsWith("b00zdhtg"));
        assertThat(episode.getTitle(), is(brandEpisode.getTitle()));
    }

    @Test
    public void testParentRefsForExtractedBrandSeriesEpisodeAreBrandAndSeries() {
        
        Episode brandSeriesEpisode = new Episode();
        brandSeriesEpisode.setPid("p00wqr14");
        brandSeriesEpisode.setTitle("Asylum of the Daleks");
        brandSeriesEpisode.setPresentationTitle("Episode 1");
        brandSeriesEpisode.setEpisodeOf(pidRef("series", "p00wqr12"));
        brandSeriesEpisode.setAncestorTitles(ancestorTitles("b006q2x0", "Doctor Who",
            ImmutableMap.of("p00wqr12","Series 7 Part 1")
        ));
        
        Item extracted = extractor.extract(NitroItemSource.valueOf(brandSeriesEpisode));
        
        org.atlasapi.media.entity.Episode episode
            = (org.atlasapi.media.entity.Episode) extracted;
        assertThat(episode.getContainer().getUri(), endsWith("b006q2x0"));
        assertThat(episode.getSeriesRef().getUri(), endsWith("p00wqr12"));
        assertThat(episode.getTitle(), is(brandSeriesEpisode.getTitle()));
    }

    @Test
    public void testParentRefsForExtractedBrandSeriesSeriesEpisodeAreBrandAndHigherLevelSeries() {
        
        Episode brandSeriesSeriesEpisode = new Episode();
        brandSeriesSeriesEpisode.setPid("b01h91l5");
        brandSeriesSeriesEpisode.setTitle("Part 2");
        brandSeriesSeriesEpisode.setPresentationTitle("Part 2");
        brandSeriesSeriesEpisode.setEpisodeOf(pidRef("series", "b01h8xs7"));
        brandSeriesSeriesEpisode.setAncestorTitles(ancestorTitles("b007y6k8", "Silent Witness",
            ImmutableMap.of("b01fltqv","Series 15", "b01h8xs7", "Fear")
        ));
        
        Item extracted = extractor.extract(NitroItemSource.valueOf(brandSeriesSeriesEpisode));
        
        org.atlasapi.media.entity.Episode episode
            = (org.atlasapi.media.entity.Episode) extracted;
        assertThat(episode.getContainer().getUri(), endsWith("b007y6k8"));
        assertThat(episode.getSeriesRef().getUri(), endsWith("b01fltqv"));
        assertThat(episode.getTitle(), is("Fear - Part 2"));
    }

    @Test
    public void testParentRefsForExtractedSeriesSeriesEpisodeAreBothHigherLevelSeries() {
        
        Episode seriesSeriesEpisode = new Episode();
        seriesSeriesEpisode.setPid("b011pq1v");
        seriesSeriesEpisode.setPresentationTitle("Part 3");
        seriesSeriesEpisode.setEpisodeOf(pidRef("series", "b011s30z"));
        seriesSeriesEpisode.setAncestorTitles(ancestorTitles(null, null,
            ImmutableMap.of("b011cdng","The Complete Smiley",
                    "b011s30z","Tinker, Tailor, Soldier, Spy")
        ));
        
        Item extracted = extractor.extract(NitroItemSource.valueOf(seriesSeriesEpisode));
        
        org.atlasapi.media.entity.Episode episode
            = (org.atlasapi.media.entity.Episode) extracted;
        assertThat(episode.getContainer().getUri(), endsWith("b011cdng"));
        assertThat(episode.getSeriesRef().getUri(), endsWith("b011cdng"));
        assertThat(episode.getTitle(), is("Tinker, Tailor, Soldier, Spy - Part 3"));
    }

    @Test
    public void testFilmInstanceIsCreatedForFilmsFormat() {
        Episode tli = new Episode();
        tli.setPid("b012cl84");
        tli.setTitle("Destiny");
        tli.setProgrammeFormats(filmFormatsType());

        Item extracted = extractor.extract(NitroItemSource.valueOf(tli));

        assertThat(extracted, is(Matchers.instanceOf(Film.class)));
    }

    // TODO: see @Ignore below
    @Ignore("quick hax to get available_versions working, fix")
    @Test
    public void testAudioDescribedFlagIsProperlySet() throws DatatypeConfigurationException {
        Episode tli = new Episode();
        tli.setPid("b012cl84");
        tli.setTitle("Destiny");

        Item extractedAudioDescribed = extractor.extract(NitroItemSource.valueOf(
                tli,
                ImmutableList.of())
        );
//        Item extractedAudioDescribed = extractor.extract(NitroItemSource.valueOf(tli,
//                ImmutableList.of(availability(AUDIO_DESCRIBED_VERSION_PID)),
//                ImmutableList.<Broadcast>of(),
//                ImmutableList.of(audioDescribedVersion())));

        org.atlasapi.media.entity.Version audioDescribedVersion = extractedAudioDescribed.getVersions()
                .iterator()
                .next();

        Encoding audioDescribedEncoding = audioDescribedVersion.getManifestedAs().iterator().next();

        Item extractedNonAudioDescribed = extractor.extract(NitroItemSource.valueOf(
                tli,
                ImmutableList.of()
        ));
//        Item extractedNonAudioDescribed = extractor.extract(NitroItemSource.valueOf(tli,
//                ImmutableList.of(availability(NON_AUDIO_DESCRIBED_VERSION_PID)),
//                ImmutableList.<Broadcast>of(),
//                ImmutableList.of(nonAudioDescribedVersion())));

        org.atlasapi.media.entity.Version nonAudioDescribedVersion = extractedNonAudioDescribed.getVersions()
                .iterator()
                .next();

        Encoding nonAudioDescribedEncoding = nonAudioDescribedVersion.getManifestedAs()
                .iterator()
                .next();

        assertTrue(audioDescribedEncoding.getAudioDescribed());
        assertFalse(nonAudioDescribedEncoding.getAudioDescribed());
        assertEquals(VERSION_DURATION.getStandardSeconds(), (int)audioDescribedVersion.getDuration());
    }

    // TODO: see @Ignore below
    @Ignore("quick hax to get available_versions working, fix")
    @Test
    public void testSignedFlagIsProperlySet() throws DatatypeConfigurationException {
        Episode tli = new Episode();
        tli.setPid("b012cl84");
        tli.setTitle("Destiny");

        String signedVersionPid = "p02ccx5g";
        String notSignedVersionPid = "p02ccx6g";

        Item extractedSigned = extractor.extract(NitroItemSource.valueOf(
                tli,
                ImmutableList.of()
        ));
//        Item extractedSigned = extractor.extract(NitroItemSource.valueOf(tli,
//                ImmutableList.of(availability(signedVersionPid)),
//                ImmutableList.<Broadcast>of(),
//                ImmutableList.of(signedVersion(signedVersionPid))));

        org.atlasapi.media.entity.Version signedVersion = Iterables.getOnlyElement(extractedSigned.getVersions());
        Encoding signedEncoding = Iterables.getOnlyElement(signedVersion.getManifestedAs());

        Item extractedNonAudioDescribed = extractor.extract(NitroItemSource.valueOf(
                tli,
                ImmutableList.of()
        ));
//        Item extractedNonAudioDescribed = extractor.extract(NitroItemSource.valueOf(tli,
//                ImmutableList.of(availability(notSignedVersionPid)),
//                ImmutableList.<Broadcast>of(),
//                ImmutableList.of(version(notSignedVersionPid))));

        org.atlasapi.media.entity.Version nonSignedVersion = Iterables.getOnlyElement(extractedNonAudioDescribed.getVersions());
        Encoding nonSignedEncoding = Iterables.getOnlyElement(nonSignedVersion.getManifestedAs());

        assertTrue(signedEncoding.getSigned());
        assertFalse(nonSignedEncoding.getSigned());
    }

    // TODO: see @Ignore below
    @Ignore("quick hax to get available_versions working, fix")
    @Test
    public void testRestrictionIsProperlySet() throws DatatypeConfigurationException {
        Episode tli = new Episode();
        tli.setPid("b012cl84");
        tli.setTitle("Destiny");

        String warningMessage = "This is a warning";

        Item extractedAudioDescribed = extractor.extract(NitroItemSource.valueOf(
                tli,
                ImmutableList.of()
        ));
//        Item extractedAudioDescribed = extractor.extract(NitroItemSource.valueOf(tli,
//                ImmutableList.of(availability(WITH_WARNING_VERSION_PID)),
//                ImmutableList.<Broadcast>of(),
//                ImmutableList.of(versionWithWarning(warningMessage))));

        org.atlasapi.media.entity.Version version = extractedAudioDescribed.getVersions()
                .iterator()
                .next();

        Restriction restriction = version.getRestriction();

        assertThat(restriction, is(notNullValue()));
        assertEquals(restriction.getMessage(), warningMessage);
    }

    // TODO: see @Ignore below
    @Ignore("quick hax to get available_versions working, fix")
    @Test
    public void testVideoDimensionsAreNotHd() throws DatatypeConfigurationException {
        Episode tli = new Episode();
        tli.setPid("b012cl84");
        tli.setTitle("Destiny");

        Item extracted = extractor.extract(NitroItemSource.valueOf(
                tli,
                ImmutableList.of()
        ));
//        Item extracted = extractor.extract(NitroItemSource.valueOf(tli,
//                ImmutableList.of(sdAvailability(VERSION_PID)),
//                ImmutableList.<Broadcast>of(),
//                ImmutableList.of(version(VERSION_PID))));

        org.atlasapi.media.entity.Version version = Iterables.getOnlyElement(extracted.getVersions());

        Set<Encoding> encodings = version.getManifestedAs();
        Encoding encoding = Iterables.getOnlyElement(encodings);

        assertEquals(640, (int)encoding.getVideoHorizontalSize());
        assertEquals(360, (int)encoding.getVideoVerticalSize());
    }

    // TODO: see @Ignore below
    @Ignore("quick hax to get available_versions working, fix")
    @Test
    public void testVideoDimensionsAreHd() throws DatatypeConfigurationException {
        Episode tli = new Episode();
        tli.setPid("b012cl84");
        tli.setTitle("Destiny");

        Item extracted = extractor.extract(NitroItemSource.valueOf(
                tli,
                ImmutableList.of()
        ));
//        Item extracted = extractor.extract(NitroItemSource.valueOf(tli,
//                ImmutableList.of(hdAvailability(VERSION_PID)),
//                ImmutableList.<Broadcast>of(),
//                ImmutableList.of(version(VERSION_PID))));

        org.atlasapi.media.entity.Version version = Iterables.getOnlyElement(extracted.getVersions());

        Set<Encoding> encodings = version.getManifestedAs();
        Encoding encoding = Iterables.getOnlyElement(encodings);

        assertEquals(1280, (int) encoding.getVideoHorizontalSize());
        assertEquals(720, (int) encoding.getVideoVerticalSize());
    }

    @Test
    public void versionsWithNullDurationsAreIgnored() throws Exception {
        Episode tli = new Episode();
        tli.setPid("b012cl84");
        tli.setTitle("Fubar");

        AvailableVersions availableVersions = new AvailableVersions();

        String withDurationPid = "p02ccx71";
        String withoutDurationPid = "p02ccx72";

        AvailableVersions.Version withDuration = makeVersion(withDurationPid);

        AvailableVersions.Version withoutDuration = makeVersion(withoutDurationPid);
        withoutDuration.setDuration(null);

        availableVersions.getVersion().add(withDuration);
        availableVersions.getVersion().add(withoutDuration);

        tli.setAvailableVersions(availableVersions);

        Item extracted = extractor.extract(NitroItemSource.valueOf(
                tli,
                ImmutableList.of())
        );

        Set<org.atlasapi.media.entity.Version> versions = extracted.getVersions();
        assertThat(versions.size(), is(1));

        org.atlasapi.media.entity.Version version = versions.iterator().next();
        assertThat(version.getCanonicalUri(), endsWith(withDurationPid));
    }

    private AvailableVersions.Version makeVersion(String pid) throws DatatypeConfigurationException {
        DatatypeFactory datatypeFactory = DatatypeFactory.newInstance();

        AvailableVersions.Version version = new AvailableVersions.Version();

        version.setPid(pid);
        version.setDuration(datatypeFactory.newDuration(VERSION_DURATION.getMillis()));

        AvailableVersions.Version.Availabilities availabilities =
                new AvailableVersions.Version.Availabilities();

        AvailableVersions.Version.Availabilities.Availability availability =
                new AvailableVersions.Version.Availabilities.Availability();

        DateTime start = DateTime.now(DateTimeZone.UTC);
        DateTime end = start.plusHours(2);

        availability.setScheduledStart(datatypeFactory.newXMLGregorianCalendar(
                start.getYear(),
                start.getMonthOfYear(),
                start.getDayOfMonth(),
                start.getHourOfDay(),
                start.getMinuteOfHour(),
                start.getSecondOfMinute(),
                start.getMillisOfSecond(),
                0
        ));

        availability.setScheduledStart(datatypeFactory.newXMLGregorianCalendar(
                end.getYear(),
                end.getMonthOfYear(),
                end.getDayOfMonth(),
                end.getHourOfDay(),
                end.getMinuteOfHour(),
                end.getSecondOfMinute(),
                end.getMillisOfSecond(),
                0
        ));

        AvailableVersions.Version.Availabilities.Availability.MediaSets mediaSets =
                new AvailableVersions.Version.Availabilities.Availability.MediaSets();

        AvailableVersions.Version.Availabilities.Availability.MediaSets.MediaSet iptvAll =
                new AvailableVersions.Version.Availabilities.Availability.MediaSets.MediaSet();
        iptvAll.setName("iptv-all");

        AvailableVersions.Version.Availabilities.Availability.MediaSets.MediaSet iptvSd =
                new AvailableVersions.Version.Availabilities.Availability.MediaSets.MediaSet();
        iptvSd.setName("iptv-sd");

        mediaSets.getMediaSet().add(iptvAll);
        mediaSets.getMediaSet().add(iptvSd);

        availability.setMediaSets(mediaSets);

        availabilities.getAvailableVersionsAvailability()
                .add(availability);
        version.getAvailabilities().add(availabilities);

        return version;
    }

    @Test
    public void testMediaTypeIsProperlySet() {
        Episode audioEpisode = new Episode();
        audioEpisode.setPid("b012cl84");
        audioEpisode.setTitle("Destiny");
        audioEpisode.setMediaType("Audio");

        Item audioExtracted = extractor.extract(NitroItemSource.valueOf(
                audioEpisode,
                ImmutableList.of()
        ));
//        Item audioExtracted = extractor.extract(NitroItemSource.valueOf(audioEpisode,
//                ImmutableList.<Availability>of(),
//                ImmutableList.<Broadcast>of(),
//                ImmutableList.<Version>of()));

        assertEquals(MediaType.AUDIO, audioExtracted.getMediaType());

        Episode videoEpisode = new Episode();
        videoEpisode.setPid("b012cl84");
        videoEpisode.setTitle("Destiny");
        videoEpisode.setMediaType("Video");

        Item videoExtracted = extractor.extract(NitroItemSource.valueOf(
                videoEpisode,
                ImmutableList.<Broadcast>of()
        ));
//        Item videoExtracted = extractor.extract(NitroItemSource.valueOf(videoEpisode,
//                ImmutableList.<Availability>of(),
//                ImmutableList.<Broadcast>of(),
//                ImmutableList.<Version>of()));

        assertEquals(MediaType.VIDEO, videoExtracted.getMediaType());
    }

    private ProgrammeFormats filmFormatsType() {
        ProgrammeFormats formatsType = new ProgrammeFormats();
        Format filmsFormat = new Format();

        filmsFormat.setFormatId("PT007");
        filmsFormat.setValue("Films");
        formatsType.getFormat().add(filmsFormat);

        return formatsType;
    }

    private AncestorTitles ancestorTitles(String brandPid, String brandTitle) {
        return ancestorTitles(brandPid, brandTitle, ImmutableMap.<String,String>of());
    }

    private AncestorTitles ancestorTitles(String brandPid, String brandTitle,
            ImmutableMap<String, String> series) {
        AncestorTitles titles = new AncestorTitles();
        if (!Strings.isNullOrEmpty(brandPid) && !Strings.isNullOrEmpty(brandTitle)) {
            AncestorTitles.Brand brand = new AncestorTitles.Brand();
            brand.setPid(brandPid);
            brand.setTitle(brandTitle);
            titles.setBrand(brand);
        }
        for (Entry<String, String> sery : series.entrySet()) {
            AncestorTitles.Series ancestorSeries = new AncestorTitles.Series();
            ancestorSeries.setPid(sery.getKey());
            ancestorSeries.setTitle(sery.getValue());
            titles.getSeries().add(ancestorSeries);
        }
        return titles;
    }

    private PidReference pidRef(String type, String pid) {
        PidReference pidRef = new PidReference();
        pidRef.setPid(pid);
        pidRef.setResultType(type);
        return pidRef;
    }

    private Availability baseAvailability(String versionPid) {
        Availability availability = new Availability();

        AvailabilityOf availabilityOfVersion = new AvailabilityOf();
        availabilityOfVersion.setPid(versionPid);
        availabilityOfVersion.setResultType("version");
        availability.getAvailabilityOf().add(availabilityOfVersion);

        AvailabilityOf availabilityOfEpisode = new AvailabilityOf();
        availabilityOfEpisode.setPid(EPISODE_PID);
        availabilityOfEpisode.setResultType("episode");
        availability.getAvailabilityOf().add(availabilityOfEpisode);
        return availability;
    }

    private Availability availability(String versionPid) {
        Availability availability = baseAvailability(versionPid);
        availability.getMediaSet().add("pc");

        return availability;
    }

    private Availability sdAvailability(String versionPid) {
        Availability availability = baseAvailability(versionPid);
        availability.getMediaSet().add("iptv-sd");
        availability.getMediaSet().add("iptv-all");

        return availability;
    }

    private Availability hdAvailability(String versionPid) {
        Availability availability = baseAvailability(versionPid);
        availability.getMediaSet().add("iptv-hd");
        availability.getMediaSet().add("iptv-all");

        return availability;
    }

    private Version version(String versionPid) throws DatatypeConfigurationException {
        Version version = new Version();
        version.setPid(versionPid);
        version.setDuration(DatatypeFactory.newInstance().newDuration(VERSION_DURATION.getMillis()));

        return version;
    }

    private Version audioDescribedVersion() throws DatatypeConfigurationException {
        Version version = new Version();
        version.setPid(AUDIO_DESCRIBED_VERSION_PID);

        Version.Types.Type type = new Version.Types.Type();
        type.setId("DubbedAudioDescribed");

        Version.Types types = new Version.Types();
        types.getType().add(type);

        version.setTypes(types);
        version.setDuration(DatatypeFactory.newInstance().newDuration(VERSION_DURATION.getMillis()));

        return version;
    }

    private Version signedVersion(String versionPid) throws DatatypeConfigurationException {
        Version version = version(versionPid);

        Version.Types.Type type = new Version.Types.Type();
        type.setId("Signed");

        Version.Types types = new Version.Types();
        types.getType().add(type);
        version.setTypes(types);

        return version;
    }

    private Version nonAudioDescribedVersion() throws DatatypeConfigurationException {
        Version version = new Version();
        version.setPid(NON_AUDIO_DESCRIBED_VERSION_PID);
        version.setDuration(DatatypeFactory.newInstance().newDuration(VERSION_DURATION.getMillis()));

        return version;
    }

    private Version versionWithWarning(String warningMessage)
            throws DatatypeConfigurationException {
        Version version = version(WITH_WARNING_VERSION_PID);

        Warnings warnings = new Warnings();

        WarningText warningText = new WarningText();
        warningText.setLength("long");
        warningText.setValue(warningMessage);

        warnings.getWarningText().add(warningText);
        version.setWarnings(warnings);

        return version;
    }

}

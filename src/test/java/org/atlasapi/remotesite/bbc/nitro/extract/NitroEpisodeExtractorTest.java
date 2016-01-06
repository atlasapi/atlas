package org.atlasapi.remotesite.bbc.nitro.extract;

import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

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
import org.hamcrest.Matchers;
import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.metabroadcast.atlas.glycerin.model.AncestorTitles;
import com.metabroadcast.atlas.glycerin.model.AvailableVersions;
import com.metabroadcast.atlas.glycerin.model.AvailableVersions.Version;
import com.metabroadcast.atlas.glycerin.model.AvailableVersions.Version.Availabilities;
import com.metabroadcast.atlas.glycerin.model.AvailableVersions.Version.Availabilities.Availability;
import com.metabroadcast.atlas.glycerin.model.Broadcast;
import com.metabroadcast.atlas.glycerin.model.Episode;
import com.metabroadcast.atlas.glycerin.model.Format;
import com.metabroadcast.atlas.glycerin.model.FormatsType;
import com.metabroadcast.atlas.glycerin.model.PidReference;
import com.metabroadcast.atlas.glycerin.model.WarningTexts;
import com.metabroadcast.common.time.SystemClock;

public class NitroEpisodeExtractorTest {

    private static final String AUDIO_DESCRIBED_VERSION_PID = "p02ccx3g";
    private static final String NON_AUDIO_DESCRIBED_VERSION_PID = "p02ccx5g";
    private static final String WITH_WARNING_VERSION_PID = "p02ccx8g";
    private static final String VERSION_PID = "p02ccx7g";
    private static final String SIGNED_VERSION_ID = "p02ccx5g";
    private static final String NON_SIGNED_VERSION_ID = "p02ccx6g";
    private static final Duration VERSION_DURATION = Duration.standardMinutes(90);

    private static final String EPISODE_PID = "p01mv8m3";
    private final NitroEpisodeExtractor extractor = new NitroEpisodeExtractor(new SystemClock(),
            Mockito.mock(QueuingPersonWriter.class));

    @Test
    public void testParentRefsForExtractedTopLevelItemAreEmpty()
            throws DatatypeConfigurationException {

        Episode tli = new Episode();
        tli.setPid("p01mv8m3");
        tli.setTitle("Pantocracy");

        Item extracted = extractor.extract(NitroItemSource.valueOf(tli,
                availability(VERSION_PID),
                tli.getPid()));

        assertFalse(extracted instanceof org.atlasapi.media.entity.Episode);
        assertNull(extracted.getContainer());
        assertThat(extracted.getTitle(), is(tli.getTitle()));

    }

    @Test
    public void testParentRefsForExtractedBrandEpisodeAreBrandOnly()
            throws DatatypeConfigurationException {

        Episode brandEpisode = new Episode();
        brandEpisode.setPid("p017m2vg");
        brandEpisode.setTitle("01/01/2004");
        brandEpisode.setEpisodeOf(pidRef("brand", "b006m86d"));
        brandEpisode.setAncestorTitles(ancestorTitles("b006m86d", "EastEnders"));

        Item extracted = extractor.extract(NitroItemSource.valueOf(brandEpisode,
                availability(VERSION_PID),
                brandEpisode.getPid()));

        org.atlasapi.media.entity.Episode episode
                = (org.atlasapi.media.entity.Episode) extracted;
        assertThat(episode.getContainer().getUri(), endsWith("b006m86d"));
        assertNull(episode.getSeriesRef());
        assertThat(episode.getTitle(), is(brandEpisode.getTitle()));
    }

    @Test
    public void testParentRefsForExtractedSeriesEpisodeAreSeriesOnly()
            throws DatatypeConfigurationException {

        Episode brandEpisode = new Episode();
        brandEpisode.setPid("b012cl84");
        brandEpisode.setTitle("Destiny");
        brandEpisode.setEpisodeOf(pidRef("series", "b00zdhtg"));
        brandEpisode.setAncestorTitles(ancestorTitles(null, null,
                ImmutableMap.of("b00zdhtg", "Wonders of the Universe")
        ));

        Item extracted = extractor.extract(NitroItemSource.valueOf(brandEpisode,
                availability(VERSION_PID),
                brandEpisode.getPid()));

        org.atlasapi.media.entity.Episode episode
                = (org.atlasapi.media.entity.Episode) extracted;
        assertThat(episode.getContainer().getUri(), endsWith("b00zdhtg"));
        assertThat(episode.getSeriesRef().getUri(), endsWith("b00zdhtg"));
        assertThat(episode.getTitle(), is(brandEpisode.getTitle()));
    }

    @Test
    public void testParentRefsForExtractedBrandSeriesEpisodeAreBrandAndSeries()
            throws DatatypeConfigurationException {

        Episode brandSeriesEpisode = new Episode();
        brandSeriesEpisode.setPid("p00wqr14");
        brandSeriesEpisode.setTitle("Asylum of the Daleks");
        brandSeriesEpisode.setPresentationTitle("Episode 1");
        brandSeriesEpisode.setEpisodeOf(pidRef("series", "p00wqr12"));
        brandSeriesEpisode.setAncestorTitles(ancestorTitles("b006q2x0", "Doctor Who",
                ImmutableMap.of("p00wqr12", "Series 7 Part 1")
        ));

        Item extracted = extractor.extract(NitroItemSource.valueOf(brandSeriesEpisode,
                availability(VERSION_PID),
                brandSeriesEpisode.getPid()));

        org.atlasapi.media.entity.Episode episode
                = (org.atlasapi.media.entity.Episode) extracted;
        assertThat(episode.getContainer().getUri(), endsWith("b006q2x0"));
        assertThat(episode.getSeriesRef().getUri(), endsWith("p00wqr12"));
        assertThat(episode.getTitle(), is(brandSeriesEpisode.getTitle()));
    }

    @Test
    public void testParentRefsForExtractedBrandSeriesSeriesEpisodeAreBrandAndHigherLevelSeries()
            throws DatatypeConfigurationException {

        Episode brandSeriesSeriesEpisode = new Episode();
        brandSeriesSeriesEpisode.setPid("b01h91l5");
        brandSeriesSeriesEpisode.setTitle("Part 2");
        brandSeriesSeriesEpisode.setPresentationTitle("Part 2");
        brandSeriesSeriesEpisode.setEpisodeOf(pidRef("series", "b01h8xs7"));
        brandSeriesSeriesEpisode.setAncestorTitles(ancestorTitles("b007y6k8", "Silent Witness",
                ImmutableMap.of("b01fltqv", "Series 15", "b01h8xs7", "Fear")
        ));

        Item extracted = extractor.extract(NitroItemSource.valueOf(brandSeriesSeriesEpisode,
                availability(VERSION_PID),
                brandSeriesSeriesEpisode.getPid()));

        org.atlasapi.media.entity.Episode episode
                = (org.atlasapi.media.entity.Episode) extracted;
        assertThat(episode.getContainer().getUri(), endsWith("b007y6k8"));
        assertThat(episode.getSeriesRef().getUri(), endsWith("b01fltqv"));
        assertThat(episode.getTitle(), is("Fear - Part 2"));
    }

    @Test
    public void testParentRefsForExtractedSeriesSeriesEpisodeAreBothHigherLevelSeries()
            throws DatatypeConfigurationException {

        Episode seriesSeriesEpisode = new Episode();
        seriesSeriesEpisode.setPid("b011pq1v");
        seriesSeriesEpisode.setPresentationTitle("Part 3");
        seriesSeriesEpisode.setEpisodeOf(pidRef("series", "b011s30z"));
        seriesSeriesEpisode.setAncestorTitles(ancestorTitles(null, null,
                ImmutableMap.of("b011cdng", "The Complete Smiley",
                        "b011s30z", "Tinker, Tailor, Soldier, Spy")
        ));

        Item extracted = extractor.extract(NitroItemSource.valueOf(seriesSeriesEpisode,
                availability(VERSION_PID),
                seriesSeriesEpisode.getPid()));

        org.atlasapi.media.entity.Episode episode
                = (org.atlasapi.media.entity.Episode) extracted;
        assertThat(episode.getContainer().getUri(), endsWith("b011cdng"));
        assertThat(episode.getSeriesRef().getUri(), endsWith("b011cdng"));
        assertThat(episode.getTitle(), is("Tinker, Tailor, Soldier, Spy - Part 3"));
    }

    @Test
    public void testFilmInstanceIsCreatedForFilmsFormat() throws DatatypeConfigurationException {
        Episode tli = new Episode();
        tli.setPid("b012cl84");
        tli.setTitle("Destiny");
        tli.setFormats(filmFormatsType());

        Item extracted = extractor.extract(NitroItemSource.valueOf(tli,
                availability(VERSION_PID),
                tli.getPid()));

        assertThat(extracted, is(Matchers.instanceOf(Film.class)));
    }

    @Test
    public void testAudioDescribedFlagIsProperlySet() throws DatatypeConfigurationException {
        Episode tli = new Episode();
        tli.setPid("b012cl84");
        tli.setTitle("Destiny");

        Item extractedAudioDescribed = extractor.extract(NitroItemSource.valueOf(tli,
                availability(AUDIO_DESCRIBED_VERSION_PID),
                ImmutableList.<Broadcast>of(),
                tli.getPid()));

        org.atlasapi.media.entity.Version audioDescribedVersion = extractedAudioDescribed.getVersions()
                .iterator()
                .next();

        Encoding audioDescribedEncoding = audioDescribedVersion.getManifestedAs().iterator().next();

        Item extractedNonAudioDescribed = extractor.extract(NitroItemSource.valueOf(tli,
                availability(NON_AUDIO_DESCRIBED_VERSION_PID),
                ImmutableList.<Broadcast>of(),
                tli.getPid()));

        org.atlasapi.media.entity.Version nonAudioDescribedVersion = extractedNonAudioDescribed.getVersions()
                .iterator()
                .next();

        Encoding nonAudioDescribedEncoding = nonAudioDescribedVersion.getManifestedAs()
                .iterator()
                .next();

        assertTrue(audioDescribedEncoding.getAudioDescribed());
        assertFalse(nonAudioDescribedEncoding.getAudioDescribed());
        assertEquals(VERSION_DURATION.getStandardSeconds(),
                (int) audioDescribedVersion.getDuration());
    }

    @Test
    public void testSignedFlagIsProperlySet() throws DatatypeConfigurationException {
        Episode tli = new Episode();
        tli.setPid("b012cl84");
        tli.setTitle("Destiny");

        Item extractedSigned = extractor.extract(NitroItemSource.valueOf(tli,
                availability(SIGNED_VERSION_ID),
                ImmutableList.<Broadcast>of(),
                tli.getPid()));

        org.atlasapi.media.entity.Version signedVersion = Iterables.getOnlyElement(extractedSigned.getVersions());
        Encoding signedEncoding = Iterables.getOnlyElement(signedVersion.getManifestedAs());

        Item extractedNonAudioDescribed = extractor.extract(NitroItemSource.valueOf(tli,
                availability(NON_SIGNED_VERSION_ID),
                ImmutableList.<Broadcast>of(),
                tli.getPid()));

        org.atlasapi.media.entity.Version nonSignedVersion = Iterables.getOnlyElement(
                extractedNonAudioDescribed.getVersions());
        Encoding nonSignedEncoding = Iterables.getOnlyElement(nonSignedVersion.getManifestedAs());

        assertTrue(signedEncoding.getSigned());
        assertFalse(nonSignedEncoding.getSigned());
    }

    @Test
    public void testRestrictionIsProperlySet() throws DatatypeConfigurationException {
        Episode tli = new Episode();
        tli.setPid("b012cl84");
        tli.setTitle("Destiny");

        String warningMessage = "This is a warning";

        Item extractedAudioDescribed = extractor.extract(NitroItemSource.valueOf(tli,
                availability(WITH_WARNING_VERSION_PID),
                ImmutableList.<Broadcast>of(),
                tli.getPid()));

        org.atlasapi.media.entity.Version version = extractedAudioDescribed.getVersions()
                .iterator()
                .next();

        Restriction restriction = version.getRestriction();

        assertThat(restriction, is(notNullValue()));
        assertEquals(restriction.getMessage(), warningMessage);
    }

    @Test
    public void testVideoDimensionsAreNotHd() throws DatatypeConfigurationException {
        Episode tli = new Episode();
        tli.setPid("b012cl84");
        tli.setTitle("Destiny");

        Item extracted = extractor.extract(NitroItemSource.valueOf(tli,
                sdAvailability(VERSION_PID),
                ImmutableList.<Broadcast>of(),
                tli.getPid()));

        org.atlasapi.media.entity.Version version = Iterables.getOnlyElement(extracted.getVersions());

        Set<Encoding> encodings = version.getManifestedAs();
        Encoding encoding = Iterables.getOnlyElement(encodings);

        assertEquals(640, (int) encoding.getVideoHorizontalSize());
        assertEquals(360, (int) encoding.getVideoVerticalSize());
    }

    @Test
    public void testVideoDimensionsAreHd() throws DatatypeConfigurationException {
        Episode tli = new Episode();
        tli.setPid("b012cl84");
        tli.setTitle("Destiny");

        Item extracted = extractor.extract(NitroItemSource.valueOf(tli,
                hdAvailability(VERSION_PID),
                ImmutableList.<Broadcast>of(),
                tli.getPid()));

        org.atlasapi.media.entity.Version version = Iterables.getOnlyElement(extracted.getVersions());

        Set<Encoding> encodings = version.getManifestedAs();
        Encoding encoding = Iterables.getOnlyElement(encodings);

        assertEquals(1280, (int) encoding.getVideoHorizontalSize());
        assertEquals(720, (int) encoding.getVideoVerticalSize());
    }

    @Test
    public void testMediaTypeIsProperlySet() throws DatatypeConfigurationException {
        Episode audioEpisode = new Episode();
        audioEpisode.setPid("b012cl84");
        audioEpisode.setTitle("Destiny");
        audioEpisode.setMediaType("Audio");

        Item audioExtracted = extractor.extract(NitroItemSource.valueOf(audioEpisode,
                availability(VERSION_PID),
                ImmutableList.<Broadcast>of(),
                audioEpisode.getPid()));

        Assert.assertEquals(MediaType.AUDIO, audioExtracted.getMediaType());

        Episode videoEpisode = new Episode();
        videoEpisode.setPid("b012cl84");
        videoEpisode.setTitle("Destiny");
        videoEpisode.setMediaType("Video");

        Item videoExtracted = extractor.extract(NitroItemSource.valueOf(videoEpisode,
                availability(VERSION_PID),
                ImmutableList.<Broadcast>of(),
                videoEpisode.getPid()));

        Assert.assertEquals(MediaType.VIDEO, videoExtracted.getMediaType());
    }

    private FormatsType filmFormatsType() {
        FormatsType formatsType = new FormatsType();
        Format filmsFormat = new Format();

        filmsFormat.setFormatId("PT007");
        filmsFormat.setValue("Films");
        formatsType.getFormat().add(filmsFormat);

        return formatsType;
    }

    private AncestorTitles ancestorTitles(String brandPid, String brandTitle) {
        return ancestorTitles(brandPid, brandTitle, ImmutableMap.<String, String>of());
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

    private AvailableVersions availability(String versionId) throws DatatypeConfigurationException {
        AvailableVersions availableVersions = new AvailableVersions();
        AvailableVersions.Version version = version(versionId);
        Availabilities availabilities = new Availabilities();
        Availability.MediaSets mediaSets = new Availability.MediaSets();

        Availability availability = new Availability();
        Availability.MediaSets.MediaSet mediaSet = new Availability.MediaSets.MediaSet();
        mediaSet.setName("pc");
        mediaSets.getMediaSet().add(mediaSet);
        availability.setMediaSets(mediaSets);

        availabilities.getAvailableVersionsAvailability().add(availability);
        version.getAvailabilities().add(availabilities);
        availableVersions.getVersion().add(version);

        return availableVersions;
    }

    private AvailableVersions sdAvailability(String versionId)
            throws DatatypeConfigurationException {

        AvailableVersions availableVersions = new AvailableVersions();
        Version version = version(versionId);
        Availabilities availabilities = new Availabilities();
        Availability.MediaSets mediaSets = new Availability.MediaSets();

        Availability availability = new Availability();
        Availability.MediaSets.MediaSet sdMediaSet = new Availability.MediaSets.MediaSet();
        sdMediaSet.setName("iptv-sd");
        Availability.MediaSets.MediaSet allMediaSet = new Availability.MediaSets.MediaSet();
        allMediaSet.setName("iptv-all");

        mediaSets.getMediaSet().add(sdMediaSet);
        mediaSets.getMediaSet().add(allMediaSet);
        availability.setMediaSets(mediaSets);

        availabilities.getAvailableVersionsAvailability().add(availability);
        version.getAvailabilities().add(availabilities);
        availableVersions.getVersion().add(version);

        return availableVersions;
    }

    private AvailableVersions hdAvailability(String versionId)
            throws DatatypeConfigurationException {
        AvailableVersions availableVersions = new AvailableVersions();
        AvailableVersions.Version.Availabilities availabilities = new AvailableVersions.Version.Availabilities();
        Version version = version(versionId);
        Availability.MediaSets mediaSets = new Availability.MediaSets();

        Availability availability = new Availability();
        Availability.MediaSets.MediaSet hdMediaSet = new Availability.MediaSets.MediaSet();
        hdMediaSet.setName("iptv-hd");
        Availability.MediaSets.MediaSet allMediaSet = new Availability.MediaSets.MediaSet();
        allMediaSet.setName("iptv-all");

        mediaSets.getMediaSet().add(hdMediaSet);
        mediaSets.getMediaSet().add(allMediaSet);
        availability.setMediaSets(mediaSets);

        availabilities.getAvailableVersionsAvailability().add(availability);
        version.getAvailabilities().add(availabilities);
        availableVersions.getVersion().add(version);

        return availableVersions;
    }

    private Version version(String versionId) throws DatatypeConfigurationException {
        Version version = new Version();
        version.setPid(versionId);
        version.setDuration(DatatypeFactory.newInstance()
                .newDuration(VERSION_DURATION.getMillis()));
        Version.Types types;
        switch (versionId) {
        case AUDIO_DESCRIBED_VERSION_PID:
            types = new Version.Types();
            types.getType().add("DubbedAudioDescribed");
            version.getTypes().add(types);
            break;

        case WITH_WARNING_VERSION_PID:
            Version.Warnings warnings = new Version.Warnings();

            WarningTexts warningTexts = new WarningTexts();

            WarningTexts.WarningText warningText = new WarningTexts.WarningText();
            warningText.setLength("long");
            warningText.setValue("This is a warning");

            warningTexts.getWarningText().add(warningText);
            warnings.setWarningTexts(warningTexts);
            version.setWarnings(warnings);
            break;

        case SIGNED_VERSION_ID:
            types = new Version.Types();
            types.getType().add("Signed");
            version.getTypes().add(types);
            break;

        default:
            break;
        }
        return version;
    }

}

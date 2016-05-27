package org.atlasapi.remotesite.pa.archives;

import java.net.URISyntaxException;
import java.util.Set;

import org.atlasapi.feeds.upload.persistence.FileUploadResultStore;
import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Image;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Series;
import org.atlasapi.media.entity.Specialization;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.persistence.logging.AdapterLog;
import org.atlasapi.remotesite.pa.PaProgrammeProcessor;
import org.atlasapi.remotesite.pa.PaTagMap;
import org.atlasapi.remotesite.pa.archives.bindings.Actor;
import org.atlasapi.remotesite.pa.archives.bindings.Attr;
import org.atlasapi.remotesite.pa.archives.bindings.Billing;
import org.atlasapi.remotesite.pa.archives.bindings.Billings;
import org.atlasapi.remotesite.pa.archives.bindings.CastMember;
import org.atlasapi.remotesite.pa.archives.bindings.Category;
import org.atlasapi.remotesite.pa.archives.bindings.ProgData;
import org.atlasapi.remotesite.pa.data.PaProgrammeDataStore;

import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.time.Timestamp;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.hamcrest.Matchers;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyCollection;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class PaArchivesProgExtractorTest {
    @Mock
    private PaProgrammeDataStore store;
    @Mock
    private FileUploadResultStore resultStore;
    @Mock
    private AdapterLog log;
    @Mock
    private ContentResolver resolver;
    @Mock
    private ResolvedContent resolvedContent;
    private PaProgDataUpdatesProcessor progProcessor;
    private PaDataToUpdatesTransformer transformer;
    private final PaTagMap paTagMap = mock(PaTagMap.class);

    @Before
    public void setUp() throws URISyntaxException {
        when(resolvedContent.getFirstValue()).thenReturn(Maybe.<Identified>nothing());
        when(resolver.findByCanonicalUris(anyCollection())).thenReturn(resolvedContent);
        when(resolver.findByUris(anyCollection())).thenReturn(resolvedContent);
        progProcessor = new PaProgrammeProcessor(resolver,log,paTagMap);
        transformer = new PaDataToUpdatesTransformer();
    }

    @Test
    public void testTransformsFromArchivesToListings() throws Exception {
        ProgData archives = generateArchiveProgdata();
        org.atlasapi.remotesite.pa.listings.bindings.ProgData listing = transformer.transformToListingProgdata(archives);
        assertEquals(archives.getAttr().getAsLive(), listing.getAttr().getAsLive());
        assertEquals(archives.getAttr().getAudioDes(), listing.getAttr().getAudioDes());
        assertEquals(archives.getAttr().getFilm(), listing.getAttr().getFilm());
        assertEquals(archives.getAttr().getThreeD(), listing.getAttr().getThreeD());
        assertEquals(archives.getBillings().getBilling().size(), 2);
        org.atlasapi.remotesite.pa.listings.bindings.Billing billing = Iterables.getFirst(listing.getBillings().getBilling(), null);
        assertEquals(billing.getType(), "synopsis");
        assertEquals(billing.getvalue(), "Sci-fi adventure sequel, with Harrison Ford, Mark Hamill, Carrie Fisher, Billy Dee Williams, Alec Guinness and David Prowse.");
        assertEquals(archives.getColour(), "Colour");
        org.atlasapi.remotesite.pa.listings.bindings.CastMember castMember = Iterables.getFirst(listing.getCastMember(), null);
        org.atlasapi.remotesite.pa.listings.bindings.Actor actor = castMember.getActor();
        assertEquals(castMember.getCharacter(), "Luke Skywalker");
        assertEquals(actor.getvalue(), "Mark Hamill");
        assertEquals(actor.getPersonId(), "10012");
        org.atlasapi.remotesite.pa.listings.bindings.Category category = Iterables.getFirst(listing.getCategory(), null);
        assertEquals(category.getCategoryCode(), "1300");
        assertEquals(category.getCategoryName(), "Science Fiction");
        assertEquals(listing.getTitle(), "Star Wars Episode V: the Empire Strikes Back");
        assertEquals(listing.getRtFilmnumber(), "5217");
        assertEquals(listing.getProgId(), "263544");
        assertEquals(listing.getShowingId(), "114733341");
    }

    @Test
    public void testIngestHierarchyFromProgdata() throws Exception {
        DateTime dateTime = DateTime.now(DateTimeZone.UTC);
        ProgData archives = generateArchiveProgdata();
        org.atlasapi.remotesite.pa.listings.bindings.ProgData listing = transformer.transformToListingProgdata(archives);
        ContentHierarchyWithoutBroadcast hierarchy = progProcessor.process(listing, DateTimeZone.UTC, Timestamp.of(dateTime)).get();
        assertThat(hierarchy.getBrandSummary(), is(Optional.<Brand>absent()));
        assertThat(hierarchy.getSeriesSummary(), is(Optional.<Series>absent()));
        Item item = hierarchy.getItem();
        Set<Alias> aliases = ImmutableSet.of(new Alias("pa:film", "263544"), new Alias("rt:filmid", "5217"), new Alias("gb:pressassociation:prod:prog_id", "263544"));
        assertThat(item.getAliases(), is(aliases));
        Set<String> aliasUrls = ImmutableSet.of("http://pressassociation.com/263544");
        assertThat(item.getAliasUrls(), is(aliasUrls));
        assertThat(item.getBlackAndWhite(), is(false));
        assertThat(item.getCanonicalUri(), is("http://pressassociation.com/episodes/263544"));
        assertThat(item.getDescription(), is("Sci-fi adventure sequel, with Harrison Ford, Mark Hamill, Carrie Fisher, Billy Dee Williams, Alec Guinness and David Prowse."));
        Set<String> genres = ImmutableSet.of("http://ref.atlasapi.org/genres/atlas/drama", "http://pressassociation.com/genres/1300");
        assertThat(item.getGenres(), is(genres));
        assertThat(item.getImages(), is(Matchers.<Set<Image>>nullValue()));
        Set<String> tags = ImmutableSet.of();
        assertThat(item.getTags(), is(tags));
        assertThat(item.getLastUpdated().toString(), is(dateTime.toString()));
        assertThat(item.getYear(), is(1980));
        assertThat(item.getSpecialization(), is(Specialization.FILM));
    }

    private ProgData generateArchiveProgdata() {
        ProgData progData = new ProgData();
        progData.setShowingId("114733341");
        progData.setProgId("263544");
        progData.setRtFilmnumber("5217");
        progData.setTitle("Star Wars Episode V: the Empire Strikes Back");
        Category category = new Category();
        category.setCategoryCode("1300");
        category.setCategoryName("Science Fiction");
        progData.getCategory().add(category);
        progData.setFilmYear("1980");
        progData.setGenre("Science Fiction");
        progData.setCertificate("U");
        progData.setColour("Colour");
        progData.setStarRating("5");
        CastMember castMember = new CastMember();
        Actor actor = new Actor();
        actor.setPersonId("10012");
        actor.setvalue("Mark Hamill");
        castMember.setActor(actor);
        castMember.setCharacter("Luke Skywalker");
        progData.getCastMember().add(castMember);
        Billings billings = new Billings();
        Billing synopsis = new Billing();
        synopsis.setType("synopsis");
        synopsis.setvalue("Sci-fi adventure sequel, with Harrison Ford, Mark Hamill, Carrie Fisher, Billy Dee Williams, Alec Guinness and David Prowse.");
        Billing paDetail = new Billing();
        paDetail.setType("pa_detail1");
        paDetail.setvalue("Sci-fi adventure sequel, with Mark Hamill and Harrison Ford.");
        billings.getBilling().add(synopsis);
        billings.getBilling().add(paDetail);
        progData.setBillings(billings);
        Attr attr = new Attr();
        attr.setStereo("no");
        attr.setSubtitles("no");
        attr.setRepeat("no");
        attr.setBw("no");
        attr.setPremiere("no");
        attr.setNewEpisode("no");
        attr.setNewSeries("no");
        attr.setLastInSeries("no");
        attr.setSurround("no");
        attr.setInteractive("no");
        attr.setHd("no");
        attr.setLive("no");
        attr.setAsLive("no");
        attr.setFollowOn("no");
        attr.setTvMovie("no");
        attr.setFilm("yes");
        attr.setThreeD("no");
        attr.setSpecial("no");
        progData.setAttr(attr);
        return progData;
    }

}

package org.atlasapi.remotesite.pa;

import java.util.Iterator;

import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Described;
import org.atlasapi.media.entity.Film;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Image;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Series;
import org.atlasapi.media.entity.Version;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.persistence.content.people.ItemsPeopleWriter;
import org.atlasapi.persistence.logging.AdapterLog;
import org.atlasapi.persistence.logging.NullAdapterLog;
import org.atlasapi.remotesite.pa.listings.bindings.Attr;
import org.atlasapi.remotesite.pa.listings.bindings.PictureUsage;
import org.atlasapi.remotesite.pa.listings.bindings.Pictures;
import org.atlasapi.remotesite.pa.listings.bindings.ProgData;
import org.atlasapi.remotesite.pa.listings.bindings.Season;

import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.time.Timestamp;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.MockitoAnnotations;
import org.mockito.runners.MockitoJUnitRunner;

import static com.metabroadcast.common.time.DateTimeZones.UTC;
import static org.atlasapi.media.entity.MediaType.VIDEO;
import static org.atlasapi.media.entity.Publisher.METABROADCAST;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class PaProgrammeProcessorTest {

    private final ContentWriter contentWriter = mock(ContentWriter.class);
    private final ContentResolver contentResolver = mock(ContentResolver.class);
    private final ItemsPeopleWriter itemsPeopleWriter = mock(ItemsPeopleWriter.class);
    private final Described described = mock(Described.class);
    private final AdapterLog log = new NullAdapterLog();

    @SuppressWarnings("deprecation")
    private Channel channel = new Channel(METABROADCAST, "c", "c", false, VIDEO, "c");

    @Captor
    private ArgumentCaptor<Iterable<Image>> imageListCaptor;
    
    private PaProgrammeProcessor progProcessor;
    
    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        
        progProcessor = new PaProgrammeProcessor(contentWriter, contentResolver, log);
    }
    
    @Test 
    public void testSetsPrimaryImages() {
        Pictures pictures = new Pictures();
        
        initialisePictures(pictures);
        
        progProcessor.selectImages(pictures, described, PaProgrammeProcessor.PA_PICTURE_TYPE_SERIES, PaProgrammeProcessor.PA_PICTURE_TYPE_BRAND, Maybe.<String>nothing());
        verify(described).setImages(imageListCaptor.capture());
        verify(described).setImage(PaProgrammeProcessor.IMAGE_URL_BASE+"series1");
        verifyNoMoreInteractions(described);
        
        Iterator<Image> iter = imageListCaptor.getValue().iterator();
        assertEquals(PaProgrammeProcessor.IMAGE_URL_BASE+"series1", iter.next().getCanonicalUri());
        assertEquals(PaProgrammeProcessor.IMAGE_URL_BASE+"series2", iter.next().getCanonicalUri());
        assertEquals(PaProgrammeProcessor.IMAGE_URL_BASE+"series3", iter.next().getCanonicalUri());
        assertEquals(PaProgrammeProcessor.IMAGE_URL_BASE+"series4", iter.next().getCanonicalUri());
        assertEquals(PaProgrammeProcessor.IMAGE_URL_BASE+"series5", iter.next().getCanonicalUri());
        assertFalse(iter.hasNext());
    }
    
    @Test 
    public void testSetsPrimaryImagesSkippingFallback() {
        Pictures pictures = new Pictures();
        
        initialisePictures(pictures);
        
        progProcessor.selectImages(pictures, described, PaProgrammeProcessor.PA_PICTURE_TYPE_BRAND, PaProgrammeProcessor.PA_PICTURE_TYPE_SERIES, Maybe.<String>nothing());
        verify(described).setImages(imageListCaptor.capture());
        
        ArgumentCaptor<String> stringCaptor = ArgumentCaptor.forClass(String.class);
        verify(described, atLeastOnce()).setImage(stringCaptor.capture());
        verifyNoMoreInteractions(described);
        
        assertEquals(PaProgrammeProcessor.IMAGE_URL_BASE+"brand1", stringCaptor.getValue()); // The latest version should be...
        
        Iterator<Image> iter = imageListCaptor.getValue().iterator();
        assertEquals(PaProgrammeProcessor.IMAGE_URL_BASE+"brand1", iter.next().getCanonicalUri());
        assertEquals(PaProgrammeProcessor.IMAGE_URL_BASE+"brand2", iter.next().getCanonicalUri());
        assertEquals(PaProgrammeProcessor.IMAGE_URL_BASE+"brand3", iter.next().getCanonicalUri());
        assertEquals(PaProgrammeProcessor.IMAGE_URL_BASE+"brand4", iter.next().getCanonicalUri());
        assertFalse(iter.hasNext());
    }
    
    @Test
    public void testUsesFallbackImage() {
        Pictures pictures = new Pictures();
        
        initialisePictures(pictures);
        
        progProcessor.selectImages(pictures, described, PaProgrammeProcessor.PA_PICTURE_TYPE_EPISODE, PaProgrammeProcessor.PA_PICTURE_TYPE_SERIES, Maybe.<String>nothing());
        verify(described).setImages(imageListCaptor.capture());
        verify(described).setImage(PaProgrammeProcessor.IMAGE_URL_BASE+"series1");
        verifyNoMoreInteractions(described);
        
        Iterator<Image> iter = imageListCaptor.getValue().iterator();
        assertEquals(PaProgrammeProcessor.IMAGE_URL_BASE+"series1", iter.next().getCanonicalUri());
        assertFalse(iter.hasNext());
    }

    @Test
    public void testUsesPreferredFallbackImage() {
        Pictures pictures = new Pictures();
        
        initialisePictures(pictures);
        
        progProcessor.selectImages(pictures, described, PaProgrammeProcessor.PA_PICTURE_TYPE_EPISODE, PaProgrammeProcessor.PA_PICTURE_TYPE_SERIES, Maybe.just(PaProgrammeProcessor.PA_PICTURE_TYPE_BRAND));
        verify(described).setImages(imageListCaptor.capture());
        verify(described).setImage(PaProgrammeProcessor.IMAGE_URL_BASE+"series1");
        verifyNoMoreInteractions(described);
        
        Iterator<Image> iter = imageListCaptor.getValue().iterator();
        assertEquals(PaProgrammeProcessor.IMAGE_URL_BASE+"series1", iter.next().getCanonicalUri());
        assertFalse(iter.hasNext());
    }

    @Test
    public void testUsesPreferredFallbackImageInOrder() {
        Pictures pictures = new Pictures();
        
        initialisePictures(pictures);
        
        progProcessor.selectImages(pictures, described, PaProgrammeProcessor.PA_PICTURE_TYPE_EPISODE, PaProgrammeProcessor.PA_PICTURE_TYPE_BRAND, Maybe.just(PaProgrammeProcessor.PA_PICTURE_TYPE_SERIES));
        verify(described).setImages(imageListCaptor.capture());
        
        ArgumentCaptor<String> stringCaptor = ArgumentCaptor.forClass(String.class);
        verify(described, atLeastOnce()).setImage(stringCaptor.capture());
        verifyNoMoreInteractions(described);
        
        assertEquals(PaProgrammeProcessor.IMAGE_URL_BASE+"brand1", stringCaptor.getValue()); // The latest version should be...
        
        Iterator<Image> iter = imageListCaptor.getValue().iterator();
        assertEquals(PaProgrammeProcessor.IMAGE_URL_BASE+"brand1", iter.next().getCanonicalUri());
        assertFalse(iter.hasNext());
    }

    private void initialisePictures(Pictures pictures) {
        pictures.getPictureUsage().add(createPictureUsage(PaProgrammeProcessor.PA_PICTURE_TYPE_SERIES, "series1"));
        pictures.getPictureUsage().add(createPictureUsage(PaProgrammeProcessor.PA_PICTURE_TYPE_BRAND,  "brand1" ));
        pictures.getPictureUsage().add(createPictureUsage(PaProgrammeProcessor.PA_PICTURE_TYPE_SERIES, "series2"));
        pictures.getPictureUsage().add(createPictureUsage(PaProgrammeProcessor.PA_PICTURE_TYPE_SERIES, "series3"));
        pictures.getPictureUsage().add(createPictureUsage(PaProgrammeProcessor.PA_PICTURE_TYPE_BRAND,  "brand2" ));
        pictures.getPictureUsage().add(createPictureUsage(PaProgrammeProcessor.PA_PICTURE_TYPE_BRAND,  "brand3" ));
        pictures.getPictureUsage().add(createPictureUsage(PaProgrammeProcessor.PA_PICTURE_TYPE_SERIES, "series4"));
        pictures.getPictureUsage().add(createPictureUsage(PaProgrammeProcessor.PA_PICTURE_TYPE_SERIES, "series5"));
        pictures.getPictureUsage().add(createPictureUsage(PaProgrammeProcessor.PA_PICTURE_TYPE_BRAND,  "brand4" ));
    }
    
    private PictureUsage createPictureUsage(String type, String uri) {
        PictureUsage usage = new PictureUsage();
        usage.setType(type);
        usage.setvalue(uri);
        return usage;
    }
    

    @Test
    public void testPaSummaries() {
        int SUMMARY_BRAND_INDEX = 1;
        int SUMMARY_SERIES_INDEX = 3;

        Film film = new Film("http://pressassociation.com/films/5", "pa:f-5", Publisher.PA);

        Brand expectedItemBrand = new Brand("http://pressassociation.com/brands/5", "pa:b-5", Publisher.PA);
        Series expectedItemSeries= new Series("http://pressassociation.com/series/5-6", "pa:s-5-6", Publisher.PA);
        Brand expectedSummaryBrand = new Brand("http://summaries.pressassociation.com/brands/5", "pa:b-5", Publisher.PA_SERIES_SUMMARIES);
        Series expectedSummarySeries= new Series("http://summaries.pressassociation.com/series/5-6", "pa:s-5-6", Publisher.PA_SERIES_SUMMARIES);
        LookupRef expectedItemBrandLookupRef = LookupRef.from(expectedItemBrand);
        LookupRef expectedItemSeriesLookupRef = LookupRef.from(expectedItemSeries);

        ProgData inputProgData = setupProgData();
        
        Version version = new Version();
        version.setProvider(Publisher.PA);
        film.addVersion(version);
        
        setupContentResolver(ImmutableSet.<Identified>of(film, expectedItemBrand, expectedItemSeries));

        ContentHierarchyAndSummaries hierarchy = progProcessor.process(inputProgData, channel, UTC, Timestamp.of(0));

        assertEquals(expectedSummaryBrand, hierarchy.getBrandSummary().get());
        assertEquals(expectedSummarySeries, hierarchy.getSeriesSummary().get());

        assertThat(hierarchy.getBrandSummary().get().getEquivalentTo(), hasItem(expectedItemBrandLookupRef));
        assertThat(hierarchy.getSeriesSummary().get().getEquivalentTo(), hasItem(expectedItemSeriesLookupRef));
    }

    private ProgData setupProgData() {
        ProgData inputProgData = new ProgData();
        inputProgData.setProgId("1");
        inputProgData.setRtFilmnumber("5");
        inputProgData.setDuration("1");
        inputProgData.setDate("06/08/2012");
        inputProgData.setTime("11:40");
        Attr threeDAttr = new Attr();
        threeDAttr.setThreeD("true");
        inputProgData.setAttr(threeDAttr);
        inputProgData.setSeriesSummary("This is the series summary!");
        Season season = new Season();
        season.setSeasonSummary("This is the season summary!");
        inputProgData.setSeason(season);

        //PA Brand data
        inputProgData.setSeriesId("5");
        inputProgData.setTitle("My title");

        //PA Series data
        inputProgData.setSeriesId("5");
        inputProgData.setSeriesNumber("6");
        inputProgData.setEpisodeTotal("15");
        return inputProgData;
    }

    private void setupContentResolver(Iterable<Identified> identifieds) {
        
        for (Identified id : identifieds) {
            when(contentResolver.findByCanonicalUris(ImmutableList.of(id.getCanonicalUri())))
                .thenReturn(ResolvedContent.builder()
                .put(id.getCanonicalUri(), id)
                .build() 
            );
            if (id instanceof Series 
                    || id instanceof Brand) {
                String summaryUri = id.getCanonicalUri()
                                      .replace(Publisher.PA.key(), Publisher.PA_SERIES_SUMMARIES.key());
                when(contentResolver.findByCanonicalUris(ImmutableList.of(summaryUri)))
                    .thenReturn(ResolvedContent.builder().build()
                );
            }
        }
    }

    @Test
    @Ignore
    public void testExtractsNewFilmWithEpisodeUri() {
        ProgData progData = new ProgData();
        progData.setProgId("1");
        progData.setRtFilmnumber("5");
        progData.setDuration("1");
        progData.setDate("06/08/2012");
        progData.setTime("11:40");
        when(contentResolver.findByCanonicalUris(ImmutableList.of(
            "http://pressassociation.com/films/5",
            "http://pressassociation.com/episodes/1"
        ))).thenReturn(ResolvedContent.builder().build());
        
        progProcessor.process(progData, channel, UTC, Timestamp.of(0));

        ArgumentCaptor<Item> argCaptor = ArgumentCaptor.forClass(Item.class);
        verify(contentWriter).createOrUpdate(argCaptor.capture());
        
        Item written = argCaptor.getValue();
        
        assertThat(written.getCanonicalUri(), is("http://pressassociation.com/episodes/1"));
        assertThat(written.getCurie(), is("pa:e-1"));

        // TODO new aliases
        assertThat(written.getAliasUrls(), hasItem("http://pressassociation.com/films/5"));
        
    }

    @Test
    @Ignore
    public void testAddsEpisodesAliasForFilmWithRtFilmNumberUri() {
        ProgData progData = new ProgData();
        progData.setProgId("1");
        progData.setRtFilmnumber("5");
        progData.setDuration("1");
        progData.setDate("06/08/2012");
        progData.setTime("11:40");
        
        Film film = new Film("http://pressassociation.com/films/5", "pa:f-5", Publisher.PA);
        Version version = new Version();
        version.setProvider(Publisher.PA);
        film.addVersion(version);
        
        when(contentResolver.findByCanonicalUris(ImmutableList.of(
            "http://pressassociation.com/films/5",
            "http://pressassociation.com/episodes/1"
        ))).thenReturn(ResolvedContent.builder()
            .put("http://pressassociation.com/films/5", film)
            .build() 
        );
        
        progProcessor.process(progData, channel, UTC, Timestamp.of(0));

        ArgumentCaptor<Item> argCaptor = ArgumentCaptor.forClass(Item.class);
        verify(contentWriter).createOrUpdate(argCaptor.capture());
        
        Item written = argCaptor.getValue();
        
        // TODO new aliases
        assertThat(written.getAliasUrls(), hasItem("http://pressassociation.com/episodes/1"));
        
    }

    @Test
    @Ignore
    public void testAddsRtFilmNumberAliasForFilmWithEpisodesUri() {
        ProgData progData = new ProgData();
        progData.setProgId("1");
        progData.setRtFilmnumber("5");
        progData.setDuration("1");
        progData.setDate("06/08/2012");
        progData.setTime("11:40");
        
        Film film = new Film("http://pressassociation.com/episodes/1", "pa:e-1", Publisher.PA);
        Version version = new Version();
        version.setProvider(Publisher.PA);
        film.addVersion(version);
        
        when(contentResolver.findByCanonicalUris(ImmutableList.of(
            "http://pressassociation.com/films/5",
            "http://pressassociation.com/episodes/1"
        ))).thenReturn(ResolvedContent.builder()
            .put("http://pressassociation.com/episodes/1", film)
            .build() 
        );
        
        progProcessor.process(progData, channel, UTC, Timestamp.of(0));

        ArgumentCaptor<Item> argCaptor = ArgumentCaptor.forClass(Item.class);
        verify(contentWriter).createOrUpdate(argCaptor.capture());
        
        Item written = argCaptor.getValue();
        
        // TODO new aliases
        assertThat(written.getAliasUrls(), hasItem("http://pressassociation.com/films/5"));
        
    }

    @Test
    public void testDoesntSetGenericDescriptionFlagIfNotGeneric() {
        Film film = new Film("http://pressassociation.com/films/5", "pa:f-5", Publisher.PA);
        Version version = new Version();
        version.setProvider(Publisher.PA);
        film.addVersion(version);
        
        Brand expectedItemBrand = new Brand("http://pressassociation.com/brands/5", "pa:b-5", Publisher.PA);
        Series expectedItemSeries= new Series("http://pressassociation.com/series/5-6", "pa:s-5-6", Publisher.PA);
        setupContentResolver(ImmutableSet.<Identified>of(film, expectedItemBrand, expectedItemSeries));
        
        ProgData progData = setupProgData();
        progData.setGeneric(null);
        
        ContentHierarchyAndSummaries hierarchy = progProcessor.process(progData, channel, UTC, Timestamp.of(0));
        
        assertNull(hierarchy.getItem().getGenericDescription());
    }

    @Test
    public void testSetsGenericDescriptionFlagIfGeneric() {
        Film film = new Film("http://pressassociation.com/films/5", "pa:f-5", Publisher.PA);
        Version version = new Version();
        version.setProvider(Publisher.PA);
        film.addVersion(version);
        
        Brand expectedItemBrand = new Brand("http://pressassociation.com/brands/5", "pa:b-5", Publisher.PA);
        Series expectedItemSeries= new Series("http://pressassociation.com/series/5-6", "pa:s-5-6", Publisher.PA);
        setupContentResolver(ImmutableSet.<Identified>of(film, expectedItemBrand, expectedItemSeries));
        
        ProgData progData = setupProgData();
        progData.setGeneric("1");
        
        ContentHierarchyAndSummaries hierarchy = progProcessor.process(progData, channel, UTC, Timestamp.of(0));
        assertTrue(hierarchy.getItem().getGenericDescription());
    }
}

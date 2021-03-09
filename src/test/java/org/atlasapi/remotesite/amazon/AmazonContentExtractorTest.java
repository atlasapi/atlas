package org.atlasapi.remotesite.amazon;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.*;
import java.util.stream.Collectors;

import org.atlasapi.media.entity.*;
import org.atlasapi.media.entity.Policy.RevenueContract;
import org.atlasapi.remotesite.ContentExtractor;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.metabroadcast.common.currency.Price;
import com.metabroadcast.common.intl.Countries;
import com.metabroadcast.common.media.MimeType;


public class AmazonContentExtractorTest {

    private final ContentExtractor<AmazonItem, Iterable<Content>> extractor = new AmazonContentExtractor();

    private Map<Integer, Encoding> encodingsByHorizontalScale(Iterable<Encoding> encodings) {
        return Maps.uniqueIndex(encodings, new Function<Encoding, Integer>() {
            @Override
            public Integer apply(Encoding input) {
                return input.getVideoHorizontalSize();
            }
        });
    }

    private Map<String, Location> locationsByUrl(Iterable<Location> locations) {
        return Maps.uniqueIndex(locations, Location.TO_URI);
    }


    @Test
    public void testExtractionOfHdContent() {
        AmazonItem filmItem = createAmazonUnboxItem("filmAsin", ContentType.MOVIE)
                .withUrl("http://hdlocation.org/")
                .withUnboxHdPurchaseUrl("http://hdlocation.org/")
                .withUnboxHdPurchasePrice("9.99")
                .withUnboxSdPurchasePrice(null)
                .withUnboxSdPurchaseUrl(null)
                .build();

        Film film = (Film) Iterables.getOnlyElement(extractor.extract(filmItem));
        
        Version version = Iterables.getOnlyElement(film.getVersions());
        Encoding encoding = Iterables.getOnlyElement(version.getManifestedAs());

        assertEquals(encoding.getQuality(), org.atlasapi.media.entity.Quality.HD);
    }

    @Test
    public void testExtractionOfUhdContent() {
        AmazonItem filmItem = createAmazonUnboxItem("filmAsin", ContentType.MOVIE)
                .withUrl("http://hdlocation.org/")
                .withUnboxHdPurchaseUrl("http://hdlocation.org/")
                .withUnboxHdPurchasePrice("9.99")
                .withUnboxSdPurchasePrice(null)
                .withUnboxSdPurchaseUrl(null)
                .withTitle("Super Drugs [UHD]")
                .build();

        Film film = (Film) Iterables.getOnlyElement(extractor.extract(filmItem));

        Version version = Iterables.getOnlyElement(film.getVersions());
        Set<Encoding> encodings = version.getManifestedAs();

        boolean foundSd = false;
        boolean foundHd = false;
        boolean foundUhd = false;
        for (Encoding encoding : encodings) {
            if(encoding.getQuality().equals(org.atlasapi.media.entity.Quality.SD)){
                foundSd = true;
            }
            if(encoding.getQuality().equals(org.atlasapi.media.entity.Quality.HD)){
                foundHd = true;
            }
            if(encoding.getQuality().equals(org.atlasapi.media.entity.Quality.FOUR_K)){
                foundUhd = true;
            }
        }
        assertFalse("An SD encoding was created, but it shouldn't", foundSd);
        assertTrue("The HD encoding was not created, but it should have.", foundHd);
        assertTrue("The UHD encoding was not created, but it should have.", foundUhd);
    }

    @Test
    public void testExtractionOfNonExtractionOfUhdContent() {
        AmazonItem filmItem = createAmazonUnboxItem("filmAsin", ContentType.MOVIE)
                .withUrl("http://hdlocation.org/")
                .withUnboxHdPurchaseUrl(null)
                .withUnboxHdPurchasePrice(null)
                .withUnboxSdPurchaseUrl("http://sdlocation.org/")
                .withUnboxSdPurchasePrice("9.99")
                .withTitle("Super Drugs [UHD]")
                .build();

        Film film = (Film) Iterables.getOnlyElement(extractor.extract(filmItem));

        Version version = Iterables.getOnlyElement(film.getVersions());
        Set<Encoding> encodings = version.getManifestedAs();

        boolean foundSd = false;
        boolean foundHd = false;
        boolean foundUhd = false;
        for (Encoding encoding : encodings) {
            if(encoding.getQuality().equals(org.atlasapi.media.entity.Quality.SD)){
                foundSd = true;
            }
            if(encoding.getQuality().equals(org.atlasapi.media.entity.Quality.HD)){
                foundHd = true;
            }
            if(encoding.getQuality().equals(org.atlasapi.media.entity.Quality.FOUR_K)){
                foundUhd = true;
            }
        }
        assertTrue("An SD encoding was not created, but it should.", foundSd);
        assertFalse("The HD encoding was created, but it should not have.", foundHd);
        assertTrue("The UHD encoding was not created, but it should.", foundUhd);
    }
    
    @Test
    public void testExtractionOfGenres() {
        AmazonItem filmItem = createAmazonUnboxItem("filmAsin", ContentType.MOVIE)
                .withGenres(ImmutableSet.of(AmazonGenre.ACTION, AmazonGenre.ADVENTURE))
                .build();
        
        Content extractedContent = Iterables.getOnlyElement(extractor.extract(filmItem));
        Film film = (Film) extractedContent;
        
        assertEquals(ImmutableSet.of("http://unbox.amazon.co.uk/genres/action", "http://unbox.amazon.co.uk/genres/adventure"), film.getGenres());
    }
    
    @Test
    public void testExtractionOfPeople() {
        AmazonItem filmItem = createAmazonUnboxItem("filmAsin", ContentType.MOVIE)
                .addDirectorRole("Director 1")
                .addDirectorRole("Director 2")
                .addStarringRole("Cast 1")
                .addStarringRole("Cast 2")
                .addStarringRole("Cast 3")
                .build();

        Film film = (Film) Iterables.getOnlyElement(extractor.extract(filmItem));

        List<CrewMember> people = film.getPeople();
        Iterable<String> names = people.stream()
                .map(input -> input.name())
                .collect(Collectors.toList());
        assertEquals(ImmutableSet.of("Director 1", "Director 2", "Cast 1", "Cast 2", "Cast 3"), ImmutableSet.copyOf(names));
    }
    
    @Test
    public void testExtractionOfCommonFields() {
        AmazonItem filmItem = createAmazonUnboxItem("filmAsin", ContentType.MOVIE)
                .withTConst("ImdbId")
                .build();
        
        Film film = (Film) Iterables.getOnlyElement(extractor.extract(filmItem));
        
        assertEquals("Synopsis of the item", film.getDescription());
        assertEquals(Publisher.AMAZON_UNBOX, film.getPublisher());
        assertEquals(Specialization.FILM, film.getSpecialization());
        assertEquals(MediaType.VIDEO, film.getMediaType());
        
        assertEquals("Large Image", film.getImage());
        
        Image image = Iterables.getOnlyElement(film.getImages());
        assertEquals("Large Image", image.getCanonicalUri());
        assertEquals(ImageType.PRIMARY, image.getType());
        assertThat(image.getWidth(), is(equalTo(320)));
        assertThat(image.getHeight(), is(equalTo(240)));
        assertEquals(MimeType.IMAGE_JPG, image.getMimeType());
        assertEquals(ImageAspectRatio.FOUR_BY_THREE, image.getAspectRatio());
        
        assertThat(film.getYear(), is(equalTo(2012)));
        
        Alias imdbAlias = new Alias("zz:imdb:id", "ImdbId");
        Alias asinAlias = new Alias("gb:amazon:asin", "filmAsin");
        assertEquals(ImmutableSet.of(imdbAlias, asinAlias), film.getAliases());
        assertEquals(ImmutableSet.of("http://imdb.com/title/ImdbId", "http://gb.amazon.com/asin/filmAsin"), film.getAliasUrls());
    }

    public void testExtractionOfVersions() {
        AmazonItem filmItem = createAmazonUnboxItem("filmAsin", ContentType.MOVIE)
                .withDuration(Duration.standardMinutes(100))
                .build();

        Film film = (Film) Iterables.getOnlyElement(extractor.extract(filmItem));
        
        Version version = Iterables.getOnlyElement(film.getVersions());
        assertEquals("http://unbox.amazon.co.uk/versions/filmAsin", version.getCanonicalUri());
        assertThat(version.getDuration(), is(equalTo(100)));
    }

    @Test
    public void testExtractionOfPolicyWithRental() {
        AmazonItem filmItem = AmazonItem.builder()
                .withAsin("filmAsin")
                .withUrl("http://www.amazon.com/gp/product/B007FUIBHM/ref=atv_feed_catalog")
                .withContentType(ContentType.MOVIE)
                .withPrice("9.99")
                .withUnboxSdRentalPrice("9.99")
                .withUnboxSdRentalUrl("http://www.amazon.co.uk/gp/product/B00EV5ROP4/INSERT_TAG_HERE/ref=atv_feed_catalog/")
                .build();

        Film film = (Film) Iterables.getOnlyElement(extractor.extract(filmItem));
        
        Version version = Iterables.getOnlyElement(film.getVersions());
        Encoding encoding = Iterables.getOnlyElement(version.getManifestedAs());
        Location location = Iterables.getOnlyElement(encoding.getAvailableAt());
        Policy policy = location.getPolicy();
        
        assertEquals(RevenueContract.PAY_TO_RENT, policy.getRevenueContract());
        assertEquals(new Price(Currency.getInstance("GBP"), 9.99), policy.getPrice());
        assertEquals(ImmutableSet.of(Countries.GB), policy.getAvailableCountries());
    }

    @Test
    public void testProviderInformationSubscription() {
        AmazonItem content = AmazonItem.builder()
                .withIsTrident(true)
                .withUnboxSdRentalPrice("9.99")
                .withUnboxSdRentalUrl("http://www.amazon.co.uk/gp/product/B00EV5ROP4/INSERT_TAG_HERE/ref=atv_feed_catalog/")
                .build();

        Set<Version> versions = new AmazonContentExtractor().generateVersions(content);
        Set<Location> locations = versions.iterator().next().getManifestedAs().iterator().next().getAvailableAt();
        for (Location location : locations) {
            Provider provider = location.getProvider();
            if(location.getCanonicalUri().contains("PAY_TO_RENT")) {
                assertEquals(provider.getName(), "amazon-video");
                assertEquals(provider.getIconUrl(), "https://images.metabroadcast.com/?source=http://images.atlas.metabroadcast.com/mb-hosted-logos/amazon-video-square-logo.jpeg");
            } else if(location.getCanonicalUri().contains("SUBSCRIPTION")) {
                assertEquals(provider.getName(), "amazon-prime-video");
                assertEquals(provider.getIconUrl(), "https://images.metabroadcast.com/?source=http://images.atlas.metabroadcast.com/mb-hosted-logos/amazon-prime-video-square-logo.jpeg");
            }
        }
        assertEquals(locations.size(), 2);
    }

    @Test
    public void testProviderInformationNoSubscription() {
        AmazonItem content = AmazonItem.builder()
                .withUnboxHdRentalPrice("9.99")
                .withUnboxHdRentalUrl("http://www.amazon.co.uk/gp/product/B00EV5ROP4/INSERT_TAG_HERE/ref=atv_feed_catalog/")
                .build();

        Set<Version> versions = new AmazonContentExtractor().generateVersions(content);
        Set<Location> locations = versions.iterator().next().getManifestedAs().iterator().next().getAvailableAt();
        for (Location location : locations) {
            Provider provider = location.getProvider();
            if(location.getCanonicalUri().contains("PAY_TO_RENT")) {
                assertEquals(provider.getName(), "amazon-video");
                assertEquals(provider.getIconUrl(), "https://images.metabroadcast.com/?source=http://images.atlas.metabroadcast.com/mb-hosted-logos/amazon-video-square-logo.jpeg");
            } else if(location.getCanonicalUri().contains("SUBSCRIPTION")) {
                assertEquals(provider.getName(), "amazon-prime-video");
                assertEquals(provider.getIconUrl(), "https://images.metabroadcast.com/?source=http://images.atlas.metabroadcast.com/mb-hosted-logos/amazon-prime-video-square-logo.jpeg");
            }
        }
        assertEquals(locations.size(), 1);
    }
    
    @Test
    public void testExtractionOfPolicyWithNoSubscription() {
        AmazonItem filmItem = createAmazonUnboxItem("filmAsin", ContentType.MOVIE)
                .withTitle("testTitle")
                .withRental(false)
                .withUrl("unbox.amazon.co.uk/filmAsin")
                .withUnboxHdPurchaseUrl("unbox.amazon.co.uk/filmAsin")
                .withUnboxHdPurchasePrice("5.00")
                .build();

        Film film = (Film) Iterables.getOnlyElement(extractor.extract(filmItem));
        
        Version version = Iterables.getOnlyElement(film.getVersions());
        Encoding encoding = Iterables.getOnlyElement(version.getManifestedAs());
        Location location = Iterables.getOnlyElement(encoding.getAvailableAt());
        Policy policy = location.getPolicy();
        
        assertEquals(RevenueContract.PAY_TO_BUY, policy.getRevenueContract());
    }

    @Test
    public void testExtractionOfPolicyWithSubscription() {
        AmazonItem filmItem = createAmazonUnboxItem("filmAsin", ContentType.MOVIE)
                .withTitle("testTitle")
                .withRental(true)
                .withIsTrident(true)
                .withUrl("unbox.amazon.co.uk/filmAsin")
                .withUnboxHdPurchaseUrl("unbox.amazon.co.uk/filmAsin")
                .withUnboxHdPurchasePrice("5.00")
                .build();

        Film film = (Film) Iterables.getOnlyElement(extractor.extract(filmItem));

        Version version = Iterables.getOnlyElement(film.getVersions());
        Encoding encoding = Iterables.getOnlyElement(version.getManifestedAs());

        assertEquals(encoding.getAvailableAt().size(), 2);
        boolean foundSub = false;
        boolean foundPay = false;
        for (Location location : encoding.getAvailableAt()) {
            Policy policy = location.getPolicy();
            if(policy.getRevenueContract() == RevenueContract.SUBSCRIPTION){
                foundSub = true;
            }
            else if(policy.getRevenueContract() == RevenueContract.PAY_TO_BUY){
                foundPay = true;
            }
        }
        assertTrue("The subscription location was not generated",foundSub);
        assertTrue("The pay policy was not generated",foundPay);
    }
    
    @Test
    public void testExtractionOfFilm() {
        AmazonItem filmItem = createAmazonUnboxItem("filmAsin", ContentType.MOVIE)
                .build();
        
        
        Film film = (Film) Iterables.getOnlyElement(extractor.extract(filmItem));
        
        assertEquals("http://unbox.amazon.co.uk/filmAsin", film.getCanonicalUri());
        assertEquals("Large Image", film.getImage());
    }
    
    //TODO hierarchied episodes?
    @Test
    public void testExtractionOfEpisodeWithSeries() {
        AmazonItem episodeItem = createAmazonUnboxItem("episodeAsin", ContentType.TVEPISODE)
                .withEpisodeNumber(5)
                .withSeasonAsin("seasonAsin")
                .withSeasonNumber(2)
                .build();
        
        
        Episode episode = Iterables.getOnlyElement(Iterables.filter(extractor.extract(episodeItem), Episode.class));
                
        assertEquals("http://unbox.amazon.co.uk/episodeAsin", episode.getCanonicalUri());
        assertEquals("http://unbox.amazon.co.uk/seasonAsin", episode.getSeriesRef().getUri());
        assertThat(episode.getEpisodeNumber(), is(equalTo(5)));
        assertThat(episode.getSeriesNumber(), is(equalTo(2)));
    }

    @Test
    @Ignore //no longer required behaviour, but that might change thus left the test.
    public void testEpisodeTitleDoesNotContainBrandTitle() {
        AmazonItem episodeItem = createAmazonUnboxItem("episodeAsin", ContentType.TVEPISODE)
                .withEpisodeNumber(5)
                .withSeasonNumber(2)
                .withSeasonAsin("seasonAsin")
                .withSeriesTitle("Vivere Pericolosamente")
                .withTitle("Ep.5 - Vivere Pericolosamente")
                .build();

        Episode episode = Iterables.getOnlyElement(Iterables.filter(extractor.extract(episodeItem), Episode.class));

        assertThat(episode.getTitle(), is("Ep.5"));
    }
    
    @Test
    public void testExtractionOfEpisodeWithBrand() {
        AmazonItem episodeItem = createAmazonUnboxItem("episodeAsin", ContentType.TVEPISODE)
                .withEpisodeNumber(5)
                .withSeriesAsin("seriesAsin")
                .build();
        
        
        Episode episode = Iterables.getOnlyElement(Iterables.filter(extractor.extract(episodeItem), Episode.class));
        
        assertEquals("http://unbox.amazon.co.uk/episodeAsin", episode.getCanonicalUri());
        assertEquals("http://unbox.amazon.co.uk/seriesAsin", episode.getContainer().getUri());
        assertThat(episode.getEpisodeNumber(), is(equalTo(5)));
    }
    
    @Test
    public void testExtractionOfEpisodeWithSeriesAndBrand() {
        AmazonItem episodeItem = createAmazonUnboxItem("episodeAsin", ContentType.TVEPISODE)
                .withEpisodeNumber(5)
                .withSeasonAsin("seasonAsin")
                .withSeasonNumber(2)
                .withSeriesAsin("seriesAsin")
                .withSeriesTitle("Series")
                .build();
        
        
        Episode episode = Iterables.getOnlyElement(Iterables.filter(extractor.extract(episodeItem), Episode.class));
                
        assertEquals("http://unbox.amazon.co.uk/episodeAsin", episode.getCanonicalUri());
        assertEquals("http://unbox.amazon.co.uk/seasonAsin", episode.getSeriesRef().getUri());
        assertEquals("http://unbox.amazon.co.uk/seriesAsin", episode.getContainer().getUri());
        assertThat(episode.getEpisodeNumber(), is(equalTo(5)));
        assertThat(episode.getSeriesNumber(), is(equalTo(2)));
        
        Brand brand = Iterables.getOnlyElement(Iterables.filter(extractor.extract(episodeItem), Brand.class));
        assertThat(brand.getCanonicalUri(), is(equalTo("http://unbox.amazon.co.uk/seriesAsin")));
        assertThat(Iterables.getOnlyElement(brand.getRelatedLinks()).getUrl(), is("http://www.amazon.co.uk/dp/seriesAsin/"));
    }
    
    @Test
    public void testExtractionOfItem() {
        AmazonItem episodeItem = createAmazonUnboxItem("itemAsin", ContentType.TVEPISODE).build();
        
        Item item = (Item) Iterables.getOnlyElement(Iterables.filter(extractor.extract(episodeItem), Item.class));
        
        assertEquals("http://unbox.amazon.co.uk/itemAsin", item.getCanonicalUri());
    }
    
    @Test
    public void testExtractionOfSeriesWithBrand() {
        AmazonItem episodeItem = createAmazonUnboxItem("seasonAsin", ContentType.TVSEASON)
                .withSeriesAsin("seriesAsin")
                .build();
        
        Series series = (Series) Iterables.getOnlyElement(extractor.extract(episodeItem));
        
        assertEquals("http://unbox.amazon.co.uk/seasonAsin", series.getCanonicalUri());
        assertEquals("http://unbox.amazon.co.uk/seriesAsin", series.getParent().getUri());
    }
    
    @Test
    public void testExtractionOfTopLevelSeries() {
        AmazonItem episodeItem = createAmazonUnboxItem("seasonAsin", ContentType.TVSEASON).build();
        
        Series series = (Series) Iterables.getOnlyElement(extractor.extract(episodeItem));
        
        assertEquals("http://unbox.amazon.co.uk/seasonAsin", series.getCanonicalUri());
        assertEquals("http://unbox.amazon.co.uk/S3R1S4S1N",series.getParent().toString());
    }

    @Test
    public void testImageExtractionHandlesNullImageUris() {
        AmazonItem amazonItem =
                createAmazonUnboxItem("seasonAsin", ContentType.TVSEASON)
                        .withLargeImageUrl(null)
                        .build();

        Series series = (Series) Iterables.getOnlyElement(extractor.extract(amazonItem));

        assertEquals(0, series.getImages().size());
    }

    @Test
    public void testImageExtractionHandlesEmptyImageUris() {
        AmazonItem amazonItem =
                createAmazonUnboxItem("seasonAsin", ContentType.TVSEASON)
                        .withLargeImageUrl("")
                        .build();

        Series series = (Series) Iterables.getOnlyElement(extractor.extract(amazonItem));

        assertEquals(0, series.getImages().size());
    }

    /**
     * Creates a Builder object for an AmazonItem, defaulting enough fields to
     * ensure that content extraction will succeed. Any of these fields can be overridden,
     * and more fields can be added to the return value of this method if needed.
     * 
     * @param asin - identifier for the item being created
     * @param type - type of item
     * @return
     */
    private AmazonItem.Builder createAmazonUnboxItem(String asin, ContentType type) {
        return AmazonItem.builder()
                .withAsin(asin)
                .withTitle("testTitle")
                .withUrl("http://www.amazon.com/gp/product/B007FUIBHM/ref=atv_feed_catalog")
                .withSynopsis("Synopsis of the item")
                .withLargeImageUrl("Large Image")
                .withContentType(type)
                .withReleaseDate(new DateTime(2012, 6, 6, 0, 0, 0))
                .withQuality(Quality.SD)
                .withDuration(Duration.standardMinutes(100))
                .withPrice("9.99")
                .withSeriesAsin("S3R1S4S1N")
                .withUnboxSdPurchasePrice("9.99")
                .withUnboxSdPurchaseUrl("http://www.amazon.co.uk/gp/product/B00EV5ROP4/INSERT_TAG_HERE/ref=atv_feed_catalog/");
                
    }
}

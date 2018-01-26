package org.atlasapi.remotesite.amazonunbox;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

import java.util.Currency;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.CrewMember;
import org.atlasapi.media.entity.CrewMember.Role;
import org.atlasapi.media.entity.Encoding;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Film;
import org.atlasapi.media.entity.Image;
import org.atlasapi.media.entity.ImageAspectRatio;
import org.atlasapi.media.entity.ImageType;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Location;
import org.atlasapi.media.entity.MediaType;
import org.atlasapi.media.entity.Policy;
import org.atlasapi.media.entity.Policy.RevenueContract;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Series;
import org.atlasapi.media.entity.Specialization;
import org.atlasapi.media.entity.Version;
import org.atlasapi.remotesite.ContentExtractor;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.metabroadcast.common.currency.Price;
import com.metabroadcast.common.intl.Countries;
import com.metabroadcast.common.media.MimeType;


public class AmazonUnboxContentExtractorTest {

    private final ContentExtractor<AmazonUnboxItem, Iterable<Content>> extractor = new AmazonUnboxContentExtractor();
    
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
    
    private void assertVersionFeatures(Version version, int horizontalScale, int verticalScale, String aspectRatio, int bitRate,
            String locationUri, RevenueContract payToBuy, boolean isHd) {

        Encoding encoding = Iterables.getOnlyElement(version.getManifestedAs());
        assertThat(encoding.getVideoHorizontalSize(), is(equalTo(horizontalScale)));
        assertThat(encoding.getVideoVerticalSize(), is(equalTo(verticalScale)));
        assertEquals(aspectRatio, encoding.getVideoAspectRatio());
        assertThat(encoding.getBitRate(), is(equalTo(bitRate)));
        
    }

    @Test
    public void testExtractionOfHdContent() {
        AmazonUnboxItem filmItem = createAmazonUnboxItem("filmAsin", ContentType.MOVIE)
                .withUrl("http://hdlocation.org/")
                .withUnboxHdPurchaseUrl("http://hdlocation.org/")
                .withUnboxHdPurchasePrice("9.99")
                .withUnboxSdPurchasePrice(null)
                .withUnboxSdPurchaseUrl(null)
                .build();

        Film film = (Film) Iterables.getOnlyElement(extractor.extract(filmItem));
        
        Version version = Iterables.getOnlyElement(film.getVersions());
        Encoding encoding = Iterables.getOnlyElement(version.getManifestedAs());
        
        assertThat(encoding.getVideoHorizontalSize(), is(equalTo(1280)));
        assertThat(encoding.getVideoVerticalSize(), is(equalTo(720)));
        assertEquals("16:9", encoding.getVideoAspectRatio());
        assertThat(encoding.getBitRate(), is(equalTo(3308)));
    }

    //the test is meaningless since there is nothing on the feed and thus we (now) do nothing, and
    //this checks the nothingness of it all.
    @Ignore
    @Test
    public void testExtractionOfLanguages() {
        AmazonUnboxItem filmItem = createAmazonUnboxItem("filmAsin", ContentType.MOVIE).build();

        Film film = (Film) Iterables.getOnlyElement(extractor.extract(filmItem));

        //amazon feed does not contain languages, so no languages is extracted.
        assertEquals(ImmutableSet.of(), film.getLanguages());
    }
    
    @Test
    public void testExtractionOfGenres() {
        AmazonUnboxItem filmItem = createAmazonUnboxItem("filmAsin", ContentType.MOVIE)
                .withGenres(ImmutableSet.of(AmazonUnboxGenre.ACTION, AmazonUnboxGenre.ADVENTURE))
                .build();
        
        Content extractedContent = Iterables.getOnlyElement(extractor.extract(filmItem));
        Film film = (Film) extractedContent;
        
        assertEquals(ImmutableSet.of("http://unbox.amazon.co.uk/genres/action", "http://unbox.amazon.co.uk/genres/adventure"), film.getGenres());
    }
    
    @Test
    public void testExtractionOfPeople() {
        AmazonUnboxItem filmItem = createAmazonUnboxItem("filmAsin", ContentType.MOVIE)
                .addDirectorRole("Director")
                .addStarringRole("Cast 1")
                .addStarringRole("Cast 2")
                .addStarringRole("Cast 3")
                .build();
        
        Content extractedContent = Iterables.getOnlyElement(extractor.extract(filmItem));
        Film film = (Film) extractedContent;

        List<CrewMember> people = film.getPeople();
        Iterable<String> names = people.stream()
                .map(input -> input.name())
                .collect(Collectors.toList());
        assertEquals(ImmutableSet.of("Director", "Cast 1", "Cast 2", "Cast 3"), ImmutableSet.copyOf(names));
        
        CrewMember director = Iterables.getOnlyElement(people.stream()
                .filter(input -> input.role() == Role.DIRECTOR)
                .collect(Collectors.toList()));
        
        assertEquals(Role.DIRECTOR, director.role());
        assertEquals("Director", director.name());
    }
    
    @Test
    public void testExtractionOfCommonFields() {
        AmazonUnboxItem filmItem = createAmazonUnboxItem("filmAsin", ContentType.MOVIE)
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
        AmazonUnboxItem filmItem = createAmazonUnboxItem("filmAsin", ContentType.MOVIE)
                .withDuration(Duration.standardMinutes(100))
                .build();

        Film film = (Film) Iterables.getOnlyElement(extractor.extract(filmItem));
        
        Version version = Iterables.getOnlyElement(film.getVersions());
        assertEquals("http://unbox.amazon.co.uk/versions/filmAsin", version.getCanonicalUri());
        assertThat(version.getDuration(), is(equalTo(100)));
    }
    
    public void testExtractionOfPolicyWithRental() {
        AmazonUnboxItem filmItem = createAmazonUnboxItem("filmAsin", ContentType.MOVIE)
                .withRental(true)
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
    public void testExtractionOfPolicyWithSubscription() {
        AmazonUnboxItem filmItem = createAmazonUnboxItem("filmAsin", ContentType.MOVIE)
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
    public void testExtractionOfFilm() {
        AmazonUnboxItem filmItem = createAmazonUnboxItem("filmAsin", ContentType.MOVIE)
                .build();
        
        
        Film film = (Film) Iterables.getOnlyElement(extractor.extract(filmItem));
        
        assertEquals("http://unbox.amazon.co.uk/filmAsin", film.getCanonicalUri());
        assertEquals("Large Image", film.getImage());
    }
    
    //TODO hierarchied episodes?
    @Test
    public void testExtractionOfEpisodeWithSeries() {
        AmazonUnboxItem episodeItem = createAmazonUnboxItem("episodeAsin", ContentType.TVEPISODE)
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
    public void testExtractionOfEpisodeWithBrand() {
        AmazonUnboxItem episodeItem = createAmazonUnboxItem("episodeAsin", ContentType.TVEPISODE)
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
        AmazonUnboxItem episodeItem = createAmazonUnboxItem("episodeAsin", ContentType.TVEPISODE)
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
        AmazonUnboxItem episodeItem = createAmazonUnboxItem("itemAsin", ContentType.TVEPISODE).build();
        
        Item item = (Item) Iterables.getOnlyElement(Iterables.filter(extractor.extract(episodeItem), Item.class));
        
        assertEquals("http://unbox.amazon.co.uk/itemAsin", item.getCanonicalUri());
    }
    
    @Test
    public void testExtractionOfSeriesWithBrand() {
        AmazonUnboxItem episodeItem = createAmazonUnboxItem("seasonAsin", ContentType.TVSEASON)
                .withSeriesAsin("seriesAsin")
                .build();
        
        Series series = (Series) Iterables.getOnlyElement(extractor.extract(episodeItem));
        
        assertEquals("http://unbox.amazon.co.uk/seasonAsin", series.getCanonicalUri());
        assertEquals("http://unbox.amazon.co.uk/seriesAsin", series.getParent().getUri());
    }
    
    @Test
    public void testExtractionOfTopLevelSeries() {
        AmazonUnboxItem episodeItem = createAmazonUnboxItem("seasonAsin", ContentType.TVSEASON).build();
        
        Series series = (Series) Iterables.getOnlyElement(extractor.extract(episodeItem));
        
        assertEquals("http://unbox.amazon.co.uk/seasonAsin", series.getCanonicalUri());
        assertNull(series.getParent());
    }

    @Test
    public void testImageExtractionHandlesNullImageUris() {
        AmazonUnboxItem amazonUnboxItem =
                createAmazonUnboxItem("seasonAsin", ContentType.TVSEASON)
                        .withLargeImageUrl(null)
                        .build();

        Series series = (Series) Iterables.getOnlyElement(extractor.extract(amazonUnboxItem));

        assertEquals(0, series.getImages().size());
    }

    @Test
    public void testImageExtractionHandlesEmptyImageUris() {
        AmazonUnboxItem amazonUnboxItem =
                createAmazonUnboxItem("seasonAsin", ContentType.TVSEASON)
                        .withLargeImageUrl("")
                        .build();

        Series series = (Series) Iterables.getOnlyElement(extractor.extract(amazonUnboxItem));

        assertEquals(0, series.getImages().size());
    }

    /**
     * Creates a Builder object for an AmazonUnboxItem, defaulting enough fields to
     * ensure that content extraction will succeed. Any of these fields can be overridden,
     * and more fields can be added to the return value of this method if needed.
     * 
     * @param asin - identifier for the item being created
     * @param type - type of item
     * @return
     */
    private AmazonUnboxItem.Builder createAmazonUnboxItem(String asin, ContentType type) {
        return AmazonUnboxItem.builder()
                .withAsin(asin)
                .withUrl("http://www.amazon.com/gp/product/B007FUIBHM/ref=atv_feed_catalog")
                .withSynopsis("Synopsis of the item")
                .withLargeImageUrl("Large Image")
                .withContentType(type)
                .withReleaseDate(new DateTime(2012, 6, 6, 0, 0, 0))
                .withQuality(Quality.SD)
                .withDuration(Duration.standardMinutes(100))
                .withPrice("9.99")
                .withUnboxSdPurchasePrice("9.99")
                .withUnboxSdPurchaseUrl("http://www.amazon.co.uk/gp/product/B00EV5ROP4/INSERT_TAG_HERE/ref=atv_feed_catalog/");
                
    }
}

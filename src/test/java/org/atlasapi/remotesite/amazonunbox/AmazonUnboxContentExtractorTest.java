package org.atlasapi.remotesite.amazonunbox;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

import java.util.Currency;
import java.util.List;

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
import org.junit.Test;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.currency.Price;
import com.metabroadcast.common.intl.Countries;
import com.metabroadcast.common.media.MimeType;


public class AmazonUnboxContentExtractorTest {

    private final ContentExtractor<AmazonUnboxItem, Optional<Content>> extractor = new AmazonUnboxContentExtractor();
    
    public void testExtractionOfSdContent() {
        AmazonUnboxItem filmItem = createAmazonUnboxItem("filmAsin", ContentType.MOVIE)
                .withQuality(Quality.SD)
                .build();
        
        Film film = (Film) extractor.extract(filmItem).get();
        
        Version version = Iterables.getOnlyElement(film.getVersions());
        Encoding encoding = Iterables.getOnlyElement(version.getManifestedAs());
        
        assertThat(encoding.getVideoHorizontalSize(), is(equalTo(720)));
        assertThat(encoding.getVideoVerticalSize(), is(equalTo(576)));
        assertEquals("16:9", encoding.getVideoAspectRatio());
        assertThat(encoding.getBitRate(), is(equalTo(1600)));
    }
    
    public void testExtractionOfHdContent() {
        AmazonUnboxItem filmItem = createAmazonUnboxItem("filmAsin", ContentType.MOVIE)
                .withQuality(Quality.HD)
                .build();

        Film film = (Film) extractor.extract(filmItem).get();
        
        Version version = Iterables.getOnlyElement(film.getVersions());
        Encoding encoding = Iterables.getOnlyElement(version.getManifestedAs());
        
        assertThat(encoding.getVideoHorizontalSize(), is(equalTo(1280)));
        assertThat(encoding.getVideoVerticalSize(), is(equalTo(720)));
        assertEquals("16:9", encoding.getVideoAspectRatio());
        assertThat(encoding.getBitRate(), is(equalTo(3308)));
    }
    
    public void testExtractionOfLanguages() {
        AmazonUnboxItem filmItem = createAmazonUnboxItem("filmAsin", ContentType.MOVIE).build();

        Film film = (Film) extractor.extract(filmItem).get();
        
        assertEquals(ImmutableSet.of("en"), film.getLanguages());
    }
    
    @Test
    public void testExtractionOfGenres() {
        AmazonUnboxItem filmItem = createAmazonUnboxItem("filmAsin", ContentType.MOVIE)
                .withGenres(ImmutableSet.of(AmazonUnboxGenre.ACTION, AmazonUnboxGenre.ADVENTURE))
                .build();
        
        Content extractedContent = extractor.extract(filmItem).get();
        Film film = (Film) extractedContent;
        
        assertEquals(ImmutableSet.of("http://unbox.amazon.co.uk/genres/action", "http://unbox.amazon.co.uk/genres/adventure"), film.getGenres());
    }
    
    @Test
    public void testExtractionOfPeople() {
        AmazonUnboxItem filmItem = createAmazonUnboxItem("filmAsin", ContentType.MOVIE)
                .withDirector("Director")
                .withStarring("Cast 1")
                .withStarring("Cast 2")
                .withStarring("Cast 3")
                .build();
        
        Content extractedContent = extractor.extract(filmItem).get();
        Film film = (Film) extractedContent;

        List<CrewMember> people = film.getPeople();
        Iterable<String> names = Iterables.transform(people, new Function<CrewMember, String>() {
            @Override
            public String apply(CrewMember input) {
                return input.name();
            }
        });
        assertEquals(ImmutableSet.of("Director", "Cast 1", "Cast 2", "Cast 3"), ImmutableSet.copyOf(names));
        
        CrewMember director = Iterables.getOnlyElement(Iterables.filter(people, new Predicate<CrewMember>() {
            @Override
            public boolean apply(CrewMember input) {
                return input.role() != null;
            }}));
        
        assertEquals(Role.DIRECTOR, director.role());
        assertEquals("Director", director.name());
    }
    
    @Test
    public void testExtractionOfCommonFields() {
        AmazonUnboxItem filmItem = createAmazonUnboxItem("filmAsin", ContentType.MOVIE)
                .withTConst("ImdbId")
                .build();
        
        Film film = (Film) extractor.extract(filmItem).get();
        
        assertEquals("Synopsis of the item", film.getDescription());
        assertEquals(Publisher.AMAZON_UNBOX, film.getPublisher());
        assertEquals(Specialization.FILM, film.getSpecialization());
        assertEquals(MediaType.VIDEO, film.getMediaType());
        
        assertEquals("Large Image", film.getImage());
        
        Image image = Iterables.getOnlyElement(film.getImages());
        assertEquals("Large Image", image.getCanonicalUri());
        assertEquals(ImageType.PRIMARY, image.getType());
        assertThat(image.getWidth(), is(equalTo(180)));
        assertThat(image.getHeight(), is(equalTo(240)));
        assertEquals(MimeType.IMAGE_JPG, image.getMimeType());
        assertEquals(ImageAspectRatio.FOUR_BY_THREE, image.getAspectRatio());
        
        assertThat(film.getYear(), is(equalTo(2012)));
        
        Alias imdbAlias = new Alias("zz:imdb:id", "ImdbId");
        Alias asinAlias = new Alias("gb:amazon:asin", "filmAsin");
        assertEquals(ImmutableSet.of(imdbAlias, asinAlias), film.getAliases());
        assertEquals(ImmutableSet.of("http://www.imdb.com/title/ImdbId", "http://gb.amazon.com/asin/filmAsin"), film.getAliasUrls());
    }

    public void testExtractionOfVersions() {
        AmazonUnboxItem filmItem = createAmazonUnboxItem("filmAsin", ContentType.MOVIE)
                .withDuration(Duration.standardMinutes(100))
                .build();

        Film film = (Film) extractor.extract(filmItem).get();
        
        Version version = Iterables.getOnlyElement(film.getVersions());
        assertEquals("http://unbox.amazon.co.uk/versions/filmAsin", version.getCanonicalUri());
        assertThat(version.getDuration(), is(equalTo(100)));
    }
    
    public void testExtractionOfPolicyWithRental() {
        AmazonUnboxItem filmItem = createAmazonUnboxItem("filmAsin", ContentType.MOVIE)
                .withRental(true)
                .build();

        Film film = (Film) extractor.extract(filmItem).get();
        
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
                .build();

        Film film = (Film) extractor.extract(filmItem).get();
        
        Version version = Iterables.getOnlyElement(film.getVersions());
        Encoding encoding = Iterables.getOnlyElement(version.getManifestedAs());
        Location location = Iterables.getOnlyElement(encoding.getAvailableAt());
        Policy policy = location.getPolicy();
        
        assertEquals(RevenueContract.SUBSCRIPTION, policy.getRevenueContract());
    }
    
    @Test
    public void testExtractionOfFilm() {
        AmazonUnboxItem filmItem = createAmazonUnboxItem("filmAsin", ContentType.MOVIE)
                .build();
        
        
        Film film = (Film) extractor.extract(filmItem).get();
        
        assertEquals("http://unbox.amazon.co.uk/movies/filmAsin", film.getCanonicalUri());
    }
    
    //TODO hierarchied episodes?
    @Test
    public void testExtractionOfEpisodeWithSeries() {
        AmazonUnboxItem episodeItem = createAmazonUnboxItem("episodeAsin", ContentType.TVEPISODE)
                .withEpisodeNumber(5)
                .withSeasonAsin("seasonAsin")
                .withSeasonNumber(2)
                .build();
        
        
        Episode episode = (Episode) extractor.extract(episodeItem).get();
        
        assertEquals("http://unbox.amazon.co.uk/seasons/episodeAsin", episode.getCanonicalUri());
        assertEquals("http://unbox.amazon.co.uk/seasons/seasonAsin", episode.getSeriesRef().getUri());
        assertThat(episode.getEpisodeNumber(), is(equalTo(5)));
        assertThat(episode.getSeriesNumber(), is(equalTo(2)));
    }
    
    @Test
    public void testExtractionOfEpisodeWithBrand() {
        AmazonUnboxItem episodeItem = createAmazonUnboxItem("episodeAsin", ContentType.TVEPISODE)
                .withEpisodeNumber(5)
                .withSeriesAsin("seriesAsin")
                .build();
        
        
        Episode episode = (Episode) extractor.extract(episodeItem).get();
        
        assertEquals("http://unbox.amazon.co.uk/seasons/episodeAsin", episode.getCanonicalUri());
        assertEquals("http://unbox.amazon.co.uk/seasons/seriesAsin", episode.getContainer().getUri());
        assertThat(episode.getEpisodeNumber(), is(equalTo(5)));
    }
    
    @Test
    public void testExtractionOfEpisodeWithSeriesAndBrand() {
        AmazonUnboxItem episodeItem = createAmazonUnboxItem("episodeAsin", ContentType.TVEPISODE)
                .withEpisodeNumber(5)
                .withSeasonAsin("seasonAsin")
                .withSeasonNumber(2)
                .withSeriesAsin("seriesAsin")
                .build();
        
        
        Episode episode = (Episode) extractor.extract(episodeItem).get();
        
        assertEquals("http://unbox.amazon.co.uk/seasons/episodeAsin", episode.getCanonicalUri());
        assertEquals("http://unbox.amazon.co.uk/seasons/seasonAsin", episode.getSeriesRef().getUri());
        assertEquals("http://unbox.amazon.co.uk/seasons/seriesAsin", episode.getContainer().getUri());
        assertThat(episode.getEpisodeNumber(), is(equalTo(5)));
        assertThat(episode.getSeriesNumber(), is(equalTo(2)));
    }
    
    @Test
    public void testExtractionOfItem() {
        AmazonUnboxItem episodeItem = createAmazonUnboxItem("itemAsin", ContentType.TVEPISODE).build();
        
        Item item = (Item) extractor.extract(episodeItem).get();
        
        assertEquals("http://unbox.amazon.co.uk/seasons/itemAsin", item.getCanonicalUri());
    }
    
    @Test
    public void testExtractionOfSeriesWithBrand() {
        AmazonUnboxItem episodeItem = createAmazonUnboxItem("seasonAsin", ContentType.TVSEASON)
                .withSeriesAsin("seriesAsin")
                .build();
        
        Series series = (Series) extractor.extract(episodeItem).get();
        
        assertEquals("http://unbox.amazon.co.uk/seasons/seasonAsin", series.getCanonicalUri());
        assertEquals("http://unbox.amazon.co.uk/seasons/seriesAsin", series.getParent().getUri());
    }
    
    @Test
    public void testExtractionOfTopLevelSeries() {
        AmazonUnboxItem episodeItem = createAmazonUnboxItem("seasonAsin", ContentType.TVSEASON).build();
        
        Series series = (Series) extractor.extract(episodeItem).get();
        
        assertEquals("http://unbox.amazon.co.uk/seasons/seasonAsin", series.getCanonicalUri());
        assertNull(series.getParent());
    }
    
    @Test
    public void testExtractionOfBrand() {
        AmazonUnboxItem episodeItem = createAmazonUnboxItem("seriesAsin", ContentType.TVSERIES).build();
        
        Brand brand = (Brand) extractor.extract(episodeItem).get();
        
        assertEquals("http://unbox.amazon.co.uk/seasons/seriesAsin", brand.getCanonicalUri());
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
                .withRental(true);
    }
}

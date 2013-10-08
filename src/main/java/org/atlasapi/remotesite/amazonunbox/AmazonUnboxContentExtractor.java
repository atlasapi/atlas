package org.atlasapi.remotesite.amazonunbox;


import java.util.Currency;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringEscapeUtils;
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
import org.atlasapi.media.entity.ImageColor;
import org.atlasapi.media.entity.ImageType;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Location;
import org.atlasapi.media.entity.MediaType;
import org.atlasapi.media.entity.ParentRef;
import org.atlasapi.media.entity.Policy;
import org.atlasapi.media.entity.Policy.RevenueContract;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Series;
import org.atlasapi.media.entity.Specialization;
import org.atlasapi.media.entity.Version;
import org.atlasapi.remotesite.ContentExtractor;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.currency.Price;
import com.metabroadcast.common.intl.Countries;
import com.metabroadcast.common.media.MimeType;


public class AmazonUnboxContentExtractor implements ContentExtractor<AmazonUnboxItem, Optional<Content>> {
    
    private static final String LANGUAGE_ENGLISH = "en";
    private static final String IMDB_NAMESPACE = "zz:imdb:id";
    private static final String ASIN_NAMESPACE = "gb:amazon:asin";
    private static final String IMDB_ALIAS_URL_PREFIX = "http://www.imdb.com/title/%s";
    private static final String AMAZON_ALIAS_URL_VERSION = "http://gb.amazon.com/asin/%s";
    private static final String URI_VERSION = "http://unbox.amazon.co.uk/seasons/%s";
    private static final String FILM_URI_VERSION = "http://unbox.amazon.co.uk/movies/%s";
    private static final String VERSION_URI_PATTERN = "http://unbox.amazon.co.uk/versions/%s";
    private static final String GENRE_URI_PATTERN = "http://unbox.amazon.co.uk/genres/%s";
    
    public static final String createBrandUri(String asin) { 
        return String.format(URI_VERSION, asin);
    }
    
    public static final String createSeriesUri(String asin) { 
        return String.format(URI_VERSION, asin);
    }
    
    public static final String createEpisodeUri(String asin) { 
        return String.format(URI_VERSION, asin);
    }
    
    public static final String createFilmUri(String asin) {
        return String.format(FILM_URI_VERSION, asin);
    }

    @Override
    public Optional<Content> extract(AmazonUnboxItem source) {
        if(ContentType.MOVIE.equals(source.getContentType())) {
            return extractFilm(source);                
        }
        if (ContentType.TVSERIES.equals(source.getContentType())) {
            return extractBrand(source);
        }
        if (ContentType.TVSEASON.equals(source.getContentType())) {
            return extractSeries(source);
        }
        if (ContentType.TVEPISODE.equals(source.getContentType())) {
            return extractEpisode(source);                
        }
        return Optional.absent();
    }

    private Optional<Content> extractEpisode(AmazonUnboxItem source) {
        Item item;
        if (source.getSeasonAsin() != null || source.getSeriesAsin() != null) {
            Episode episode = new Episode();
            if (source.getEpisodeNumber() != null) {
                episode.setEpisodeNumber(source.getEpisodeNumber());
            }
            episode.setSeriesRef(new ParentRef(createSeriesUri(source.getSeasonAsin())));
            if (source.getSeasonNumber() != null) {
                episode.setSeriesNumber(source.getSeasonNumber());
            }
            episode.setParentRef(new ParentRef(createBrandUri(source.getSeriesAsin())));
            
            item = episode;
        } else {
            item = new Item();
        }
        item.setCanonicalUri(createEpisodeUri(source.getAsin()));
        setCommonFields(item, source);
        item.setSpecialization(Specialization.TV);
        return Optional.<Content>of(item);
    }

    private Optional<Content> extractSeries(AmazonUnboxItem source) {
        Series series = new Series();
        series.setCanonicalUri(createSeriesUri(source.getAsin()));
        if (source.getSeasonNumber() != null) {
            series.withSeriesNumber(source.getSeasonNumber());
        }
        if (source.getSeriesAsin() != null) {
            series.setParentRef(new ParentRef(createBrandUri(source.getSeriesAsin())));
        }
        series.setSpecialization(Specialization.TV);
        setCommonFields(series, source);
        return Optional.<Content>of(series);
    }

    private Optional<Content> extractBrand(AmazonUnboxItem source) {
        Brand brand = new Brand();
        brand.setCanonicalUri(createBrandUri(source.getAsin()));
        setCommonFields(brand, source);
        brand.setSpecialization(Specialization.TV);
        return Optional.<Content>of(brand);
    }

    private Optional<Content> extractFilm(AmazonUnboxItem source) {
        Film film = new Film();
        film.setCanonicalUri(createFilmUri(source.getAsin()));
        setCommonFields(film, source);
        film.setSpecialization(Specialization.FILM);
        film.setVersions(generateVersions(source));
        return Optional.<Content>of(film);
    }
    
    private Set<Version> generateVersions(AmazonUnboxItem source) {
        Version version = new Version();
        version.setCanonicalUri(String.format(VERSION_URI_PATTERN, source.getAsin()));
        if (source.getDuration() != null) {
            version.setDuration(source.getDuration());
        }
        
        Location location = new Location();
        // TODO determine location links, if any
        location.setPolicy(generatePolicy(source));
        
        Encoding encoding = new Encoding();
        if (Quality.SD.equals(source.getQuality())) {
            encoding.setVideoHorizontalSize(720);
            encoding.setVideoVerticalSize(576);
            encoding.setVideoAspectRatio("16:9");
            encoding.setBitRate(1600);
        } else if (Quality.HD.equals(source.getQuality())) {
            encoding.setVideoHorizontalSize(1280);
            encoding.setVideoVerticalSize(720);
            encoding.setVideoAspectRatio("16:9");
            encoding.setBitRate(3308);
        }
        encoding.setAvailableAt(ImmutableSet.of(location));
        
        version.setManifestedAs(ImmutableSet.of(encoding));
        return ImmutableSet.of(version);
    }
    
    private Policy generatePolicy(AmazonUnboxItem source) {
        Policy policy = new Policy();
        if (source.isRental()) {
            policy.setRevenueContract(RevenueContract.PAY_TO_RENT);
        } else {
            policy.setRevenueContract(RevenueContract.SUBSCRIPTION);
        }
        if (source.getPrice() != null) {
            policy.withPrice(new Price(Currency.getInstance("GBP"), Double.valueOf(source.getPrice())));
        }
        policy.setAvailableCountries(ImmutableSet.of(Countries.GB));
        return policy;
    }

    private void setCommonFields(Content content, AmazonUnboxItem source) {
        content.setGenres(generateGenres(source));
        // TODO ratings
        content.setLanguages(generateLanguages(source));
        content.setActivelyPublished(true);
        content.setMediaType(MediaType.VIDEO);
        content.setTitle(source.getTitle());
        content.setPublisher(Publisher.AMAZON_UNBOX);
        content.setDescription(StringEscapeUtils.unescapeXml(source.getSynopsis()));
        content.setImage(source.getLargeImageUrl());
        content.setImages(generateImages(source));
        if (source.getReleaseDate() != null) {
            content.setYear(source.getReleaseDate().getYear());
        }
        content.setAliases(generateAliases(source));
        content.setAliasUrls(generateAliasUrls(source));
        content.setPeople(generatePeople(source));
    }

    private Set<String> generateGenres(AmazonUnboxItem source) {
        return ImmutableSet.copyOf(Iterables.transform(
                source.getGenres(), 
                new Function<AmazonUnboxGenre, String>() {
                    @Override
                    public String apply(AmazonUnboxGenre input) {
                        return String.format(GENRE_URI_PATTERN, input.name().toLowerCase());
                    }
                }
        ));
    }

    /**
     * @param source supplied for completeness, so that the signature doesn't need changing if 
     * languages are ingested at a later point  
     */
    private Set<String> generateLanguages(AmazonUnboxItem source) {
        return ImmutableSet.of(LANGUAGE_ENGLISH);
    }

    private List<CrewMember> generatePeople(AmazonUnboxItem source) {
        if (source.getDirector() == null && source.getStarring().isEmpty()) {
            return ImmutableList.of();
        }
        Builder<CrewMember> people = ImmutableList.<CrewMember>builder();
        if (source.getDirector() != null) {
            CrewMember director = new CrewMember();
            director.withPublisher(Publisher.AMAZON_UNBOX);
            director.withName(source.getDirector());
            director.withRole(Role.DIRECTOR);
            people.add(director);
        }
        for (String role : source.getStarring()) {
            CrewMember star = new CrewMember();
            star.withPublisher(Publisher.AMAZON_UNBOX);
            star.withName(role);
            people.add(star);
        }
        return people.build();
    }

    private List<Alias> generateAliases(AmazonUnboxItem item) {
        Alias asinAlias = new Alias(ASIN_NAMESPACE, item.getAsin());
        if (item.getTConst() == null) {
            return ImmutableList.of(asinAlias);
        }
        return ImmutableList.of(asinAlias, new Alias(IMDB_NAMESPACE, item.getTConst()));
    }

    private List<String> generateAliasUrls(AmazonUnboxItem item) {
        String amazonAsinAlias = String.format(AMAZON_ALIAS_URL_VERSION, item.getAsin());
        if (item.getTConst() == null) {
            return ImmutableList.of(amazonAsinAlias);
        }
        return ImmutableList.of(amazonAsinAlias, String.format(IMDB_ALIAS_URL_PREFIX, item.getTConst()));
    }

    private List<Image> generateImages(AmazonUnboxItem item) {
        Image image = new Image(item.getLargeImageUrl());
        image.setType(ImageType.PRIMARY);
        image.setWidth(180);
        image.setHeight(240);
        image.setAspectRatio(ImageAspectRatio.FOUR_BY_THREE);
        image.setColor(ImageColor.COLOR);
        image.setMimeType(MimeType.IMAGE_JPG);
        return ImmutableList.of(image);
    }

}

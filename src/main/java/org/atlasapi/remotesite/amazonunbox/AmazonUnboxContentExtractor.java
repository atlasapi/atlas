package org.atlasapi.remotesite.amazonunbox;

import java.util.Currency;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Certificate;
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
import org.atlasapi.media.entity.RelatedLink;
import org.atlasapi.media.entity.Series;
import org.atlasapi.media.entity.Specialization;
import org.atlasapi.media.entity.Version;
import org.atlasapi.remotesite.ContentExtractor;

import com.metabroadcast.common.collect.ImmutableOptionalMap;
import com.metabroadcast.common.collect.OptionalMap;
import com.metabroadcast.common.currency.Price;
import com.metabroadcast.common.intl.Countries;
import com.metabroadcast.common.media.MimeType;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.commons.lang.StringEscapeUtils;
import org.joda.time.DateTime;

public class AmazonUnboxContentExtractor implements ContentExtractor<AmazonUnboxItem,
                                                    Iterable<Content>> {

    private static final String LANGUAGE_ENGLISH = "en";
    private static final String IMDB_NAMESPACE = "zz:imdb:id";
    private static final String ASIN_NAMESPACE = "gb:amazon:asin";
    private static final String IMDB_ALIAS_URL_PREFIX = "http://imdb.com/title/%s";
    private static final String AMAZON_ALIAS_URL_VERSION = "http://gb.amazon.com/asin/%s";
    public static final String URI_PREFIX = "http://unbox.amazon.co.uk/";
    private static final String URI_VERSION = URI_PREFIX + "%s";
    private static final String LOCATION_URI_PATTERN = "http://www.amazon.co.uk/dp/%s/";
    private static final String SERIES_URI_PATTERN = LOCATION_URI_PATTERN;
    private static final String URL_SUFFIX_TO_REMOVE = "ref=atv_feed_catalog";
    private static final String TAG_PLACEHOLDER = "INSERT_TAG_HERE/ref=atv_feed_catalog/";
    private static final String GENRE_URI_PATTERN = "http://unbox.amazon.co.uk/genres/%s";

    //since they have no end dates, but we check for an end-date, add a very very long end date.
    private static final DateTime POLICY_AVAILABILITY_ENDS = new DateTime(DateTime.parse("2100-01-10T01:11:11"));
    private static final DateTime POLICY_AVAILABILITY_START = new DateTime(DateTime.parse("2000-01-10T01:11:11"));

    private static final OptionalMap<String, Certificate> certificateMap =
            ImmutableOptionalMap.fromMap(
                    ImmutableMap.<String,Certificate>builder()
     // tba/NR temporarily set to '18' to prevent unsuitable material from being misclassified.
                .put("NR",new Certificate("18", Countries.GB))
                .put("to_be_announced",new Certificate("18", Countries.GB))
                .put("universal",new Certificate("U", Countries.GB))
                .put("parental_guidance",new Certificate("PG", Countries.GB))
                .put("ages_12_and_over",new Certificate("12", Countries.GB))
                .put("ages_15_and_over",new Certificate("15", Countries.GB))
                .put("ages_18_and_over",new Certificate("18", Countries.GB))
            .build()
        );
    
    public static String createBrandUri(String asin) {
        return String.format(URI_VERSION, asin);
    }
    
    public static String createSeriesUri(String asin) {
        return String.format(URI_VERSION, asin);
    }
    
    public static String createEpisodeUri(String asin) {
        return String.format(URI_VERSION, asin);
    }
    
    public static String createFilmUri(String asin) {
        return String.format(URI_VERSION, asin);
    }

    @Override
    public Iterable<Content> extract(AmazonUnboxItem source) {
        if(ContentType.MOVIE.equals(source.getContentType())) {
            return ImmutableSet.of(extractFilm(source));
        }
        if (ContentType.TVSERIES.equals(source.getContentType())) {
            return ImmutableSet.of(extractBrand(source));
        }
        if (ContentType.TVSEASON.equals(source.getContentType())) {
            return ImmutableSet.of(extractSeries(source));
        }
        if (ContentType.TVEPISODE.equals(source.getContentType())) {
            // Brands are not in the Unbox feed, so we must
            // create them from the data we have for an episode
            return ImmutableSet.of(extractEpisode(source), extractBrand(source));                
        }
        return ImmutableSet.of();
    }

    private Content extractEpisode(AmazonUnboxItem source) {
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
            episode.setSpecialization(Specialization.TV);
            
            item = episode;
        } else {
            item = new Item();
        }
        item.setSpecialization(Specialization.TV);
        item.setVersions(generateVersions(source));
        setFieldsForNonSynthesizedContent(item, source, createEpisodeUri(source.getAsin()));
        return item;
    }

    private Content extractSeries(AmazonUnboxItem source) {
        Series series = new Series();
        if (source.getSeasonNumber() != null) {
            series.withSeriesNumber(source.getSeasonNumber());
        }
        if (source.getSeriesAsin() != null) {
            series.setParentRef(new ParentRef(createBrandUri(source.getSeriesAsin())));
        }
        series.setSpecialization(Specialization.TV);
        setFieldsForNonSynthesizedContent(series, source, createSeriesUri(source.getAsin()));
        return series;
    }

    private Content extractBrand(AmazonUnboxItem source) {
        Brand brand = new Brand();
        setCommonFields(brand, source.getSeriesTitle(), createBrandUri(source.getSeriesAsin()));
        brand.setDescription(StringEscapeUtils.unescapeXml(source.getSynopsis()));
        brand.setSpecialization(Specialization.TV);
        
        RelatedLink relatedLink = RelatedLink
                                     .vodLink(String.format(
                                             SERIES_URI_PATTERN,
                                             source.getSeriesAsin()
                                     ))
                                     .build();
        
        brand.setRelatedLinks(ImmutableSet.of(relatedLink));
        brand.setImage(source.getLargeImageUrl());
        return brand;
    }

    private Content extractFilm(AmazonUnboxItem source) {
        Film film = new Film();
        setFieldsForNonSynthesizedContent(film, source, createFilmUri(source.getAsin()));
        film.setSpecialization(Specialization.FILM);
        film.setVersions(generateVersions(source));
        return film;
    }
    
    private Set<Version> generateVersions(AmazonUnboxItem source) {
        Set<Location> hdLocations = Sets.newHashSet();
        Set<Location> sdLocations = Sets.newHashSet();

        //We will choose the url that is in any of the
        String representingUrl = "http://www.amazon.co.uk/gp/product/"+source.getAsin()+"/";

        // PURCHASE URLS
        if (!Strings.isNullOrEmpty(source.getUnboxHdPurchaseUrl())
            && !Strings.isNullOrEmpty(source.getUnboxHdPurchasePrice())) {
            hdLocations.add(createLocation(
                    RevenueContract.PAY_TO_BUY,
                    source.getUnboxHdPurchasePrice(),
                    source.getUnboxHdPurchaseUrl()
            ));
            if (Boolean.TRUE.equals(source.isTrident())) { //available through subscription
                hdLocations.add(createLocation(
                        RevenueContract.SUBSCRIPTION,
                        null,
                        source.getUnboxHdPurchaseUrl()
                ));
            }
        } else if (!Strings.isNullOrEmpty(source.getUnboxSdPurchaseUrl())
                   && !Strings.isNullOrEmpty(source.getUnboxSdPurchasePrice())) {
            sdLocations.add(createLocation(
                    RevenueContract.PAY_TO_BUY,
                    source.getUnboxSdPurchasePrice(),
                    source.getUnboxSdPurchaseUrl()
            ));
            if (Boolean.TRUE.equals(source.isTrident())) {
                sdLocations.add(createLocation(
                        RevenueContract.SUBSCRIPTION,
                        null,
                        source.getUnboxSdPurchaseUrl()
                ));
            }
            //RENT URLS
        } else if (!Strings.isNullOrEmpty(source.getUnboxHdRentalUrl())
                   && !Strings.isNullOrEmpty(source.getUnboxHdRentalPrice())) {
            hdLocations.add(createLocation(
                    RevenueContract.PAY_TO_RENT,
                    source.getUnboxHdRentalPrice(),
                    source.getUnboxHdRentalUrl()
            ));
            if (Boolean.TRUE.equals(source.isTrident())) {
                hdLocations.add(createLocation(
                        RevenueContract.SUBSCRIPTION,
                        null,

                        source.getUnboxHdRentalUrl()
                ));
            }
        } else if (!Strings.isNullOrEmpty(source.getUnboxSdRentalUrl())
                   && !Strings.isNullOrEmpty(source.getUnboxSdRentalPrice())) {
            sdLocations.add(createLocation(
                    RevenueContract.PAY_TO_RENT,
                    source.getUnboxSdRentalPrice(),
                    source.getUnboxSdRentalUrl()
            ));
            if (Boolean.TRUE.equals(source.isTrident())) {
                sdLocations.add(createLocation(
                        RevenueContract.SUBSCRIPTION,
                        null,
                        source.getUnboxSdRentalUrl()
                ));
            }
        }

        ImmutableSet.Builder<Encoding> encodings = ImmutableSet.builder();
        if (!hdLocations.isEmpty()) {
            encodings.add(createEncoding(true, hdLocations));
        }
        if (!sdLocations.isEmpty()) {
            encodings.add(createEncoding(false, sdLocations));
        }
        
        return ImmutableSet.of(createVersion(source, representingUrl, encodings.build()));
    }

    private Version createVersion(AmazonUnboxItem source, String url, Set<Encoding> encodings) {
        Version version = new Version();
        version.setCanonicalUri(cleanUri(url));
        if (source.getDuration() != null) {
            version.setDuration(source.getDuration());
        }
        version.setManifestedAs(encodings);
        return version;
    }
    
    private Encoding createEncoding(boolean isHd,
            Set<Location> locations) {
        
        Encoding encoding = new Encoding();
        if (isHd) {
            encoding.setVideoHorizontalSize(1280);
            encoding.setVideoVerticalSize(720);
            encoding.setVideoAspectRatio("16:9");
            encoding.setBitRate(3308);
            encoding.setHighDefinition(true);
        } else {
            encoding.setVideoHorizontalSize(720);
            encoding.setVideoVerticalSize(576);
            encoding.setVideoAspectRatio("16:9");
            encoding.setBitRate(1600);
        }
        
        encoding.setAvailableAt(locations);
        return encoding;
    }
    
    private Location createLocation(RevenueContract revenueContract,
            @Nullable String price, String url) {
        String cleanedUri = cleanUri(url);

        Location location = new Location();
        location.setPolicy(generatePolicy(revenueContract, price));
        location.setUri(cleanedUri);
        location.setCanonicalUri(cleanedUri);
        return location;
    }

    private String cleanUri(String url) {
        if(url == null){
            return null;
        }
        return url.replaceAll(TAG_PLACEHOLDER, "").replaceAll(URL_SUFFIX_TO_REMOVE, "");
    }

    private Policy generatePolicy(
            RevenueContract revenueContract,
            @Nullable String price
    ) {
        Policy policy = new Policy();
        policy.setRevenueContract(revenueContract);
        if (price != null) {
            policy.withPrice(new Price(Currency.getInstance("GBP"), Double.valueOf(price)));
        }
        policy.setAvailableCountries(ImmutableSet.of(Countries.GB));

        //THE CODE BELOW, IS BASED ON THE EXISTING STATE WHERE AMAZON ALWAYS SENDS NULLS.
        // We will mark all content as available to youview. This is the
        // way that the YV uploader will then pick them up as ondemands.
        policy.setPlatform(Policy.Platform.YOUVIEW_AMAZON);
        //And for a similar reason add a very long end date to that policy
        //This needs to be a static date, because otherwise the policy will be marked as changed
        //between ingests and the item will be marked as updated, while in truth
        //amazon would have changed nothing.
        policy.setAvailabilityEnd(POLICY_AVAILABILITY_ENDS);
        policy.setAvailabilityStart(POLICY_AVAILABILITY_START);


        return policy;
    }
    
    private void setCommonFields(Content content, String title, String uri) {
        content.setCanonicalUri(uri);
        content.setActivelyPublished(true);
        content.setMediaType(MediaType.VIDEO);
        content.setPublisher(Publisher.AMAZON_UNBOX);
        content.setTitle(title);
    }

    private void setFieldsForNonSynthesizedContent(
            Content content,
            AmazonUnboxItem source,
            String uri
    ) {
        setCommonFields(content, source.getTitle(), uri);
        content.setGenres(generateGenres(source));
        content.setLanguages(generateLanguages(source));
        
        content.setDescription(StringEscapeUtils.unescapeXml(source.getSynopsis()));
        //we are setting title of brand as description for deduping episodes that
        // have same title, episode number and series number such as "Pilot Ep.1 S.1"
        content.setShortDescription(source.getSeriesTitle());
        content.setImage(source.getLargeImageUrl());
        content.setImages(generateImages(source));
        if (source.getReleaseDate() != null) {
            content.setYear(source.getReleaseDate().getYear());
        }
        content.setCertificates(generateCertificates(source));
        content.setAliases(generateAliases(source));
        content.setAliasUrls(generateAliasUrls(source));
        content.setPeople(generatePeople(source));
    }

    private Set<String> generateGenres(AmazonUnboxItem source) {
        return ImmutableSet.copyOf(source.getGenres()
                .stream()
                .map(input -> String.format(GENRE_URI_PATTERN, input.name().toLowerCase()))
                .collect(Collectors.toList()));
    }

    /**
     * @param source supplied for completeness, so that the signature doesn't need changing if 
     * languages are ingested at a later point  
     */
    private Set<String> generateLanguages(AmazonUnboxItem source) { //NOSONAR
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
        return ImmutableList.of(
                amazonAsinAlias,
                String.format(IMDB_ALIAS_URL_PREFIX, item.getTConst())
        );
    }

    private List<Image> generateImages(AmazonUnboxItem item) {
        if (Strings.isNullOrEmpty(item.getLargeImageUrl())) {
            return ImmutableList.of();
        }
        Image image = new Image(item.getLargeImageUrl());
        image.setType(ImageType.PRIMARY);
        image.setWidth(320);
        image.setHeight(240);
        image.setAspectRatio(ImageAspectRatio.FOUR_BY_THREE);
        image.setColor(ImageColor.COLOR);
        image.setMimeType(MimeType.IMAGE_JPG);
        return ImmutableList.of(image);
    }

    private Iterable<Certificate> generateCertificates(AmazonUnboxItem item) {
        return certificateMap.get(item.getRating()).asSet();
    }
}

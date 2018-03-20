package org.atlasapi.remotesite.amazonunbox;

import java.util.Currency;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.atlasapi.remotesite.amazonunbox.ContentType.MOVIE;
import static org.atlasapi.remotesite.amazonunbox.ContentType.TVEPISODE;
import static org.atlasapi.remotesite.amazonunbox.ContentType.TVSEASON;

public class AmazonUnboxContentExtractor implements ContentExtractor<AmazonUnboxItem,
                                                    Iterable<Content>> {

    private static final Logger log = LoggerFactory.getLogger(AmazonUnboxContentExtractor.class);

    private static final String IMDB_NAMESPACE = "zz:imdb:id";
    private static final String IMDB_ALIAS_URL_PREFIX = "http://imdb.com/title/%s";
    private static final String AMAZON_ALIAS_URL_VERSION = "http://gb.amazon.com/asin/%s";
    public static final String URI_PREFIX = "http://unbox.amazon.co.uk/";
    private static final String URI_VERSION = URI_PREFIX + "%s";
    private static final String LOCATION_URI_PATTERN = "http://www.amazon.co.uk/dp/%s/";
    private static final String SERIES_URI_PATTERN = LOCATION_URI_PATTERN;
    private static final String URL_SUFFIX_TO_REMOVE = "ref=atv_feed_catalog";
    private static final String TAG_PLACEHOLDER = "INSERT_TAG_HERE/ref=atv_feed_catalog/";
    private static final String GENRE_URI_PATTERN = "http://unbox.amazon.co.uk/genres/%s";

    //because they need dates in order to generate onDemands.
    private static final DateTime POLICY_AVAILABILITY_ENDS = new DateTime(DateTime.parse("2100-01-10T01:11:11"));
    private static final DateTime POLICY_AVAILABILITY_START = new DateTime(DateTime.parse("2000-01-10T01:11:11"));

    //You can use grep TITLE  GBAmazonUnboxCatalog-2017-12-12.xml | g -o "\[.*\]" | sort | uniq
    //to search the xml file and see if any new tags have been added.
    private static final Pattern UHD_PATTERN =
            Pattern.compile("\\[Ultra HD\\]|\\[ULTRA HD\\]|\\[UHD\\]|\\[4K\\/Ultra HD\\]|\\[4K\\/UHD\\]|\\[4K\\]");

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
        if(MOVIE.equals(source.getContentType())) {
            return ImmutableSet.of(extractFilm(source));
        }
        //as things currently are, these are not in the feed, and the extractBrand() method will not
        //properly cope with this. If things change amend accordingly.
//        if (TVSERIES.equals(source.getContentType())) {
//            return ImmutableSet.of(extractBrand(source));
//        }
        else if (TVSEASON.equals(source.getContentType())) {
            return ImmutableSet.of(extractSeries(source));
        }
        else if (TVEPISODE.equals(source.getContentType())) {
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
        //TODO: This is probably what creates a /null uri in the db, which is a problem that needs
        //solving.
        String brandAsin = source.getSeriesAsin();

        Brand brand = new Brand();
        brand.setSpecialization(Specialization.TV);

        RelatedLink relatedLink = RelatedLink
                .vodLink(String.format(
                        SERIES_URI_PATTERN,
                        brandAsin
                ))
                .build();
        brand.setRelatedLinks(ImmutableSet.of(relatedLink));

        setCommonFields(brand, source.getSeriesTitle(), createBrandUri(brandAsin));
        brand.setAliases(generateBrandAliases(brandAsin));

        //if this is being synthesized, which it is if we are creating a brand from an episode.
        if (TVEPISODE.equals(source.getContentType())) {
            // The description needs to be generated by a common field between all episodes.
            // otherwise the brand description will differ from ingest to ingest depending on the
            // order we ingest episodes. This would causes the brand to be marked as updated and
            // get reuploaded to YV all the time.
            brand.setDescription(source.getSeriesTitle());
            //and for the same reason, the image will be assigned later by using one from the series
        } else {
            brand.setDescription(StringEscapeUtils.unescapeXml(source.getSynopsis()));
            brand.setImage(source.getLargeImageUrl());
        }
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
        //There are 4 sources of information about the actual quality
        //1. does the title contain a UHD tag?
        //2. does the seriesTitle contain a UHD tag?
        //3. Do we have buy and rent links for either quality?
        //4. The actual quality sent by amazon.
        boolean isUhd = isUhd(source);

        Set<Location> hdLocations = Sets.newHashSet();
        Set<Location> sdLocations = Sets.newHashSet();

        //We will create the version id based on of the item itself.
        String versionUrl = String.format(AMAZON_ALIAS_URL_VERSION, source.getAsin()) + "/";
        boolean addedSubscription = false;

        // ---HD URLS---
        if (!Strings.isNullOrEmpty(source.getUnboxHdPurchaseUrl())
            && !Strings.isNullOrEmpty(source.getUnboxHdPurchasePrice())) {
            hdLocations.add(createLocation(
                    RevenueContract.PAY_TO_BUY,
                    source.getUnboxHdPurchasePrice(),
                    source.getUnboxHdPurchaseUrl()
            ));
            if (Boolean.TRUE.equals(source.isTrident())) { //available through subscription
                hdLocations.add(createSubLocation(source.getUnboxHdPurchaseUrl()));
                addedSubscription = true;
            }
        } else if (!Strings.isNullOrEmpty(source.getUnboxHdRentalUrl())
                   && !Strings.isNullOrEmpty(source.getUnboxHdRentalPrice())) {
            hdLocations.add(createLocation(
                    RevenueContract.PAY_TO_RENT,
                    source.getUnboxHdRentalPrice(),
                    source.getUnboxHdRentalUrl()
            ));
            if (Boolean.TRUE.equals(source.isTrident())) {
                hdLocations.add(createSubLocation(source.getUnboxHdRentalUrl()));
                addedSubscription = true;
            }
        } //---SD URLS--- (its an else-if because currently one asin = one SD/HD RENT/BUY combo.)
        else if (!Strings.isNullOrEmpty(source.getUnboxSdPurchaseUrl())
                  && !Strings.isNullOrEmpty(source.getUnboxSdPurchasePrice())) {
            sdLocations.add(createLocation(
                    RevenueContract.PAY_TO_BUY,
                    source.getUnboxSdPurchasePrice(),
                    source.getUnboxSdPurchaseUrl()
            ));
            if (Boolean.TRUE.equals(source.isTrident())) {
                sdLocations.add(createSubLocation(source.getUnboxSdPurchaseUrl()));
                addedSubscription = true;
            }
        }  else if (!Strings.isNullOrEmpty(source.getUnboxSdRentalUrl())
                   && !Strings.isNullOrEmpty(source.getUnboxSdRentalPrice())) {
            sdLocations.add(createLocation(
                    RevenueContract.PAY_TO_RENT,
                    source.getUnboxSdRentalPrice(),
                    source.getUnboxSdRentalUrl()
            ));
            if (Boolean.TRUE.equals(source.isTrident())) {
                sdLocations.add(createSubLocation(source.getUnboxSdRentalUrl()));
                addedSubscription = true;
            }
        }

        // if isTrident is set, but we did not find any specific URLS in the to_rent or to_buy
        // sections, we'll use the generic url of the version to create the subscription location.
        // (as per this version of the ingester, they should be the same url anyway)
        if (!addedSubscription && Boolean.TRUE.equals(source.isTrident())) {
            if (Quality.HD.equals(source.getQuality())) {
                hdLocations.add(createSubLocation(versionUrl));
            } else {
                sdLocations.add(createSubLocation(versionUrl));
            }
        }


        ImmutableSet.Builder<Encoding> encodings = ImmutableSet.builder();
        if (!hdLocations.isEmpty()) {
            if(isUhd) { //if it is uhd, and you have HD locations, replace them.
                encodings.add(createEncoding(org.atlasapi.media.entity.Quality.FOUR_K, hdLocations));
            } else {
                encodings.add(createEncoding(org.atlasapi.media.entity.Quality.HD, hdLocations));
            }
        }
        if (!sdLocations.isEmpty()) {
            //if it uhd and it comes as sd, don't mark anything, cause the title is the same
            //across SD and HD. If they have UHD, they ought to have HD, so the UHD counterpart to
            //SD will be added when processing the HD version.
            encodings.add(createEncoding(org.atlasapi.media.entity.Quality.SD, sdLocations));
        }
        return ImmutableSet.of(createVersion(source, versionUrl, encodings.build()));
    }

    private boolean isUhd(AmazonUnboxItem source) {
        if (!Strings.isNullOrEmpty(source.getTitle())
            && UHD_PATTERN.matcher(source.getTitle()).find()) {
            return true;
        }

        if (!Strings.isNullOrEmpty(source.getSeriesTitle())
            && UHD_PATTERN.matcher(source.getSeriesTitle()).find()) {
            return true;
        }

        return false;
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
    
    private Encoding createEncoding(org.atlasapi.media.entity.Quality quality,
            Set<Location> locations) {
        
        Encoding encoding = new Encoding();
        if (quality != org.atlasapi.media.entity.Quality.SD) {
            encoding.setHighDefinition(true);
        }

        encoding.setQuality(quality);
        encoding.setAvailableAt(locations);
        return encoding;
    }
    
    private Location createLocation(RevenueContract revenueContract,
            @Nullable String price, String url) {
        String cleanedUri = cleanUri(url);

        Location location = new Location();
        location.setPolicy(generatePolicy(revenueContract, price));
        location.setUri(cleanedUri);
        location.setCanonicalUri(createCannonicalUri(cleanedUri, revenueContract));
        return location;
    }

    private Location createSubLocation(String url) {
        return createLocation(RevenueContract.SUBSCRIPTION, null, url);
    }

    /**
     * The plan is to create a unique identifier for each locations, so can fit in the same set
     * multiple locations with the same uri, but different revenue methods.
     * @return
     * @param cleanedUri
     * @param revenueContract
     */
    private String createCannonicalUri(
            String cleanedUri, RevenueContract revenueContract) {
        return cleanedUri + revenueContract;
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
        content.setTitle(cleanTitle(title));
    }

    private void setFieldsForNonSynthesizedContent(
            Content content,
            AmazonUnboxItem source,
            String uri
    ) {
        setCommonFields(content,source.getTitle(), uri);
        content.setGenres(generateGenres(source));
        content.setLanguages(generateLanguages(source));
        String desc = StringEscapeUtils.unescapeXml(source.getSynopsis());
        if(Strings.isNullOrEmpty(desc)){
            content.setDescription(desc);
        } else {
            content.setDescription(source.getTitle());
        }

        // we are setting title of brand as description for deduping episodes that
        // have same title, episode number and series number such as "Pilot Ep.1 S.1"
        if (source.getSeriesTitle() != null && !source.getSeriesTitle().isEmpty()) {
            content.setShortDescription(source.getSeriesTitle());
        } else { //but if that is blank, use the episode title (YV requires a synopsis)
            content.setShortDescription(source.getTitle());
        }
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

    /**
     * Remove tags from the title, i.e. Quality info such as [UHD]
     */
    @Nullable
    private String cleanTitle(@Nullable String title) {
        if (title == null) {
            return null;
        }
        Matcher matcher = UHD_PATTERN.matcher(title);
        if (matcher.find()) {
            return matcher.replaceAll("").trim();
        }
        return title;
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
        // YV requires the language to be correct. We ingest nothing so we store nothing. YV output
        // code will detect the lack of language and publish the content as UND (i.e. undefined).
        return ImmutableSet.of();
    }

    private List<CrewMember> generatePeople(AmazonUnboxItem source) {
        if (source.getDirectors().isEmpty() && source.getStarring().isEmpty()) {
            return ImmutableList.of();
        }
        Builder<CrewMember> people = ImmutableList.<CrewMember>builder();
        for (String directorName : source.getDirectors()) {
            CrewMember director = new CrewMember();
            director.withPublisher(Publisher.AMAZON_UNBOX);
            director.withName(directorName);
            director.withRole(Role.DIRECTOR);
            people.add(director);
        }
        for (String actorName : source.getStarring()) {
            CrewMember actor = new CrewMember();
            actor.withPublisher(Publisher.AMAZON_UNBOX);
            actor.withName(actorName);
            actor.withRole(Role.ACTOR);
            people.add(actor);
        }
        return people.build();
    }

    private List<Alias> generateAliases(AmazonUnboxItem item) {
        Alias asinAlias = new Alias(AmazonUnboxContentWritingItemProcessor.GB_AMAZON_ASIN, item.getAsin());
        if (item.getTConst() == null) {
            return ImmutableList.of(asinAlias);
        }
        return ImmutableList.of(asinAlias, new Alias(IMDB_NAMESPACE, item.getTConst()));
    }

    private List<Alias> generateBrandAliases(String asin) {
        Alias asinAlias = new Alias(AmazonUnboxContentWritingItemProcessor.GB_AMAZON_ASIN, asin);
        return ImmutableList.of(asinAlias);
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

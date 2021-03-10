package org.atlasapi.remotesite.amazon;

import java.util.Currency;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import org.atlasapi.media.entity.*;
import org.atlasapi.media.entity.CrewMember.Role;
import org.atlasapi.media.entity.Policy.RevenueContract;
import org.atlasapi.remotesite.ContentExtractor;

import com.metabroadcast.common.currency.Price;
import com.metabroadcast.common.intl.Countries;
import com.metabroadcast.common.media.MimeType;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.commons.lang.StringEscapeUtils;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.atlasapi.remotesite.amazon.ContentType.MOVIE;
import static org.atlasapi.remotesite.amazon.ContentType.TVEPISODE;
import static org.atlasapi.remotesite.amazon.ContentType.TVSEASON;

public class AmazonContentExtractor implements ContentExtractor<AmazonItem,
        Iterable<Content>> {

    public static final String AMAZON_ICON_URL = "https://images.metabroadcast.com/?source=http://images.atlas.metabroadcast.com/mb-hosted-logos/amazon-video-square-logo.jpeg";
    public static final String AMAZON_PRIME_ICON_URL = "https://images.metabroadcast.com/?source=http://images.atlas.metabroadcast.com/mb-hosted-logos/amazon-prime-video-square-logo.jpeg";
    public static final String AMAZON_VIDEO_PROVIDER = "amazon-video";
    public static final String AMAZON_PRIME_VIDEO_PROVIDER = "amazon-prime-video";

    private static final Logger log = LoggerFactory.getLogger(AmazonContentExtractor.class);

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

    //because the YV code requires dates in order to pick up which ondemands to generate, but amazon sends nothing.
    private static final DateTime POLICY_AVAILABILITY_START = new DateTime(DateTime.parse(
            "2018-01-01T00:00:00Z")); //<- specific date requested by YV (ECOTEST-435)
    private static final DateTime POLICY_AVAILABILITY_ENDS = new DateTime(DateTime.parse(
            "2100-01-10T00:00:00Z"));

    //You can use grep TITLE  GBAmazonUnboxCatalog-2017-12-12.xml | g -o "\[.*\]" | sort | uniq
    //to search the xml file and see if any new tags have been added.
    private static final Pattern UHD_PATTERN =
            Pattern.compile(
                    "\\[Ultra HD\\]|\\[ULTRA HD\\]|\\[UHD\\]|\\[4K\\/Ultra HD\\]|\\[4K\\/UHD\\]|\\[4K\\]");
    private static final Duration DEFAULT_DURATION = Duration.standardMinutes(45);
    private static final Duration DEFAULT_MOVIE_DURATION = Duration.standardMinutes(120);

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
    public Iterable<Content> extract(AmazonItem source) {
        if (MOVIE.equals(source.getContentType())) {
            return ImmutableSet.of(extractFilm(source));
        }
        //as things currently are, these are not in the feed, and the extractBrand() method will not
        //properly cope with this. If things change amend accordingly.
        //        if (TVSERIES.equals(source.getContentType())) {
        //            return ImmutableSet.of(extractBrand(source));
        //        }
        else if (TVSEASON.equals(source.getContentType())) {
            return ImmutableSet.of(extractSeries(source));
        } else if (TVEPISODE.equals(source.getContentType())) {
            // Brands are not in Amazon's feed, so we must
            // create them from the data we have for an episode
            Brand brand = extractBrand(source);
            if (brand == null) {
                return ImmutableSet.of(extractEpisode(source));
            } else {
                return ImmutableSet.of(extractEpisode(source), brand);
            }
        }
        return ImmutableSet.of();
    }

    private Content extractEpisode(AmazonItem source) {
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

    private Content extractSeries(AmazonItem source) {
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

    @Nullable
    private Brand extractBrand(AmazonItem source) {
        String brandAsin = source.getSeriesAsin();
        if (brandAsin == null || brandAsin.equals("")) {
            return null;
        }

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
            String synopsis = StringEscapeUtils.unescapeXml(source.getSynopsis());
            if (synopsis != null) {
                synopsis = synopsis.trim();
            }
            if (synopsis != null && !synopsis.isEmpty()) {
                brand.setDescription(synopsis);
            } else {
                brand.setDescription(source.getTitle());
            }
            brand.setImage(source.getLargeImageUrl());
        }
        return brand;

    }

    private Content extractFilm(AmazonItem source) {
        Film film = new Film();
        setFieldsForNonSynthesizedContent(film, source, createFilmUri(source.getAsin()));
        film.setSpecialization(Specialization.FILM);
        film.setVersions(generateVersions(source));
        return film;
    }

    @VisibleForTesting
    public Set<Version> generateVersions(AmazonItem source) {
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
                    source.getUnboxHdPurchaseUrl(),
                    AMAZON_VIDEO_PROVIDER,
                    AMAZON_ICON_URL
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
                    source.getUnboxHdRentalUrl(),
                    AMAZON_VIDEO_PROVIDER,
                    AMAZON_ICON_URL
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
                    source.getUnboxSdPurchaseUrl(),
                    AMAZON_VIDEO_PROVIDER,
                    AMAZON_ICON_URL
            ));
            if (Boolean.TRUE.equals(source.isTrident())) {
                sdLocations.add(createSubLocation(source.getUnboxSdPurchaseUrl()));
                addedSubscription = true;
            }
        } else if (!Strings.isNullOrEmpty(source.getUnboxSdRentalUrl())
                   && !Strings.isNullOrEmpty(source.getUnboxSdRentalPrice())) {
            sdLocations.add(createLocation(
                    RevenueContract.PAY_TO_RENT,
                    source.getUnboxSdRentalPrice(),
                    source.getUnboxSdRentalUrl(),
                    AMAZON_VIDEO_PROVIDER,
                    AMAZON_ICON_URL
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
            if (isUhd) { //add both a UHD and HD location.
                encodings.add(createEncoding(
                        org.atlasapi.media.entity.Quality.FOUR_K,
                        hdLocations
                ));
            }
            encodings.add(createEncoding(org.atlasapi.media.entity.Quality.HD, hdLocations));

        }
        if (!sdLocations.isEmpty()) {
            if (isUhd) { //add both a UHD and SD location.
                encodings.add(createEncoding(
                        org.atlasapi.media.entity.Quality.FOUR_K,
                        sdLocations
                ));
            }
            encodings.add(createEncoding(org.atlasapi.media.entity.Quality.SD, sdLocations));
        }
        return ImmutableSet.of(createVersion(source, versionUrl, encodings.build()));
    }

    private boolean isUhd(AmazonItem source) {
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

    private Version createVersion(AmazonItem source, String url, Set<Encoding> encodings) {
        Version version = new Version();
        version.setCanonicalUri(cleanUri(url));
        if (source.getDuration() != null && source.getDuration().getStandardSeconds() > 0) {
            version.setDuration(source.getDuration());
        } else {
            //add defaults (ECOTEST-428)
            if (MOVIE.equals(source.getContentType())) {
                version.setDuration(DEFAULT_MOVIE_DURATION);
            } else {
                version.setDuration(DEFAULT_DURATION);
            }
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
            @Nullable String price, String url, String providerName, String iconUrl) {
        String cleanedUri = cleanUri(url);

        Location location = new Location();
        location.setPolicy(generatePolicy(revenueContract, price));
        location.setUri(cleanedUri);
        location.setCanonicalUri(createCannonicalUri(cleanedUri, revenueContract));
        location.setProvider(new Provider(providerName, iconUrl));
        return location;
    }

    private Location createSubLocation(String url) {
        return createLocation(RevenueContract.SUBSCRIPTION, null, url, AMAZON_PRIME_VIDEO_PROVIDER, AMAZON_PRIME_ICON_URL);
    }

    /**
     * The plan is to create a unique identifier for each locations, so can fit in the same set
     * multiple locations with the same uri, but different revenue methods.
     *
     * @param cleanedUri
     * @param revenueContract
     * @return
     */
    private String createCannonicalUri(
            String cleanedUri, RevenueContract revenueContract) {
        return cleanedUri + revenueContract;
    }

    private String cleanUri(String url) {
        if (url == null) {
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
            AmazonItem source,
            String uri
    ) {
        setCommonFields(content, source.getTitle(), uri);
        content.setGenres(generateGenres(source));
        content.setLanguages(generateLanguages(source));
        String desc = StringEscapeUtils.unescapeXml(source.getSynopsis());
        if (desc != null) {
            desc = desc.trim();
        }
        if (!Strings.isNullOrEmpty(desc)) {
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
        //Amazon sends some items with date 2049-12-20. This invalid, and we suppress" it.
        if (source.getReleaseDate() != null && source.getReleaseDate().getYear() != 2049) {
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

    private Set<String> generateGenres(AmazonItem source) {
        return ImmutableSet.copyOf(source.getGenres()
                .stream()
                .map(input -> String.format(GENRE_URI_PATTERN, input.name().toLowerCase()))
                .collect(Collectors.toList()));
    }

    /**
     * @param source supplied for completeness, so that the signature doesn't need changing if
     *               languages are ingested at a later point
     */
    private Set<String> generateLanguages(AmazonItem source) { //NOSONAR
        // YV requires the language to be correct. We ingest nothing so we store nothing. YV output
        // code will detect the lack of language and publish the content as UND (i.e. undefined).
        return ImmutableSet.of();
    }

    private List<CrewMember> generatePeople(AmazonItem source) {
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

    private List<Alias> generateAliases(AmazonItem item) {
        Alias asinAlias = new Alias(
                AmazonContentWritingItemProcessor.GB_AMAZON_ASIN,
                item.getAsin()
        );
        if (item.getTConst() == null) {
            return ImmutableList.of(asinAlias);
        }
        return ImmutableList.of(asinAlias, new Alias(IMDB_NAMESPACE, item.getTConst()));
    }

    private List<Alias> generateBrandAliases(String asin) {
        Alias asinAlias = new Alias(AmazonContentWritingItemProcessor.GB_AMAZON_ASIN, asin);
        return ImmutableList.of(asinAlias);
    }

    private List<String> generateAliasUrls(AmazonItem item) {
        String amazonAsinAlias = String.format(AMAZON_ALIAS_URL_VERSION, item.getAsin());
        if (item.getTConst() == null) {
            return ImmutableList.of(amazonAsinAlias);
        }
        return ImmutableList.of(
                amazonAsinAlias,
                String.format(IMDB_ALIAS_URL_PREFIX, item.getTConst())
        );
    }

    private List<Image> generateImages(AmazonItem item) {
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

    private Iterable<Certificate> generateCertificates(AmazonItem item) {
        if (!Strings.isNullOrEmpty(item.getRating())) {
            return ImmutableSet.of(new Certificate(item.getRating(), Countries.GB));
        } else {
            return ImmutableSet.of();
        }
    }
}

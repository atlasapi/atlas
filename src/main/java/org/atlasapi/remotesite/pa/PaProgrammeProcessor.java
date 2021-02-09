package org.atlasapi.remotesite.pa;

import java.util.List;
import java.util.Set;

import org.atlasapi.genres.GenreMap;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.entity.Actor;
import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.Certificate;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.CrewMember;
import org.atlasapi.media.entity.Described;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Film;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Image;
import org.atlasapi.media.entity.ImageAspectRatio;
import org.atlasapi.media.entity.ImageType;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.media.entity.MediaType;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Series;
import org.atlasapi.media.entity.Specialization;
import org.atlasapi.media.entity.TopicRef;
import org.atlasapi.media.entity.Version;
import org.atlasapi.media.util.ItemAndBroadcast;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.logging.AdapterLog;
import org.atlasapi.persistence.logging.AdapterLogEntry;
import org.atlasapi.persistence.logging.AdapterLogEntry.Severity;
import org.atlasapi.remotesite.pa.archives.ContentHierarchyWithoutBroadcast;
import org.atlasapi.remotesite.pa.archives.PaProgDataUpdatesProcessor;
import org.atlasapi.remotesite.pa.deletes.ExistingItemUnPublisher;
import org.atlasapi.remotesite.pa.listings.bindings.Billing;
import org.atlasapi.remotesite.pa.listings.bindings.CastMember;
import org.atlasapi.remotesite.pa.listings.bindings.PictureUsage;
import org.atlasapi.remotesite.pa.listings.bindings.Pictures;
import org.atlasapi.remotesite.pa.listings.bindings.ProgData;
import org.atlasapi.remotesite.pa.listings.bindings.StaffMember;

import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.intl.Countries;
import com.metabroadcast.common.media.MimeType;
import com.metabroadcast.common.text.MoreStrings;
import com.metabroadcast.common.time.Timestamp;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.atlasapi.persistence.logging.AdapterLogEntry.errorEntry;
import static org.atlasapi.persistence.logging.AdapterLogEntry.warnEntry;

public class PaProgrammeProcessor implements PaProgDataProcessor, PaProgDataUpdatesProcessor {

    private static final Logger log = LoggerFactory.getLogger(PaProgrammeProcessor.class);

    @VisibleForTesting
    static final String PA_PICTURE_TYPE_EPISODE = "episode";

    // Counter-intuitively PA use 'series' where we use 'container'
    @VisibleForTesting
    static final String PA_PICTURE_TYPE_BRAND = "series";

    // .. and 'season' where we use 'series'
    @VisibleForTesting
    static final String PA_PICTURE_TYPE_SERIES = "season";

    @VisibleForTesting
    static final String IMAGE_URL_BASE =
            "http://images.atlas.metabroadcast.com/pressassociation.com/";

    @VisibleForTesting
    static final String BILLING_DESCRIPTION = "synopsis";

    private static final String BILLING_SHORT_DESCRIPTION = "pa_detail1";
    private static final String BILLING_MEDIUM_DESCRIPTION = "pa_detail2";
    private static final String BILLING_LONG_DESCRIPTION = "pa_detail3";

    private static final DateTimeFormatter PA_DATE_FORMAT = DateTimeFormat.forPattern(
            "dd/MM/yyyy"
    );

    private static final String YES = "yes";
    private static final String CLOSED_BRAND = "http://pressassociation.com/brands/8267";
    private static final String SCHEDULED_ONLY_EPISODE =
            "http://pressassociation.com/episodes/1607805";
    private static final String CLOSED_EPISODE_PREFIX =
            "http://pressassociation.com/episodes/closed";
    private static final String CLOSED_CURIE = "pa:closed";

    // 70214 is 'TBA' container, 84575 is 'Film TBA'
    private static final List<String> IGNORED_BRANDS = ImmutableList.of("70214", "84575");

    private final ContentResolver contentResolver;
    private final AdapterLog adapterLog;
    private final PaCountryMap countryMap = new PaCountryMap();

    private final GenreMap genreMap = new PaGenreMap();
    private final PaTagMap paTagMap;
    private final ExistingItemUnPublisher existingItemUnPublisher;

    private PaProgrammeProcessor(
            ContentResolver contentResolver,
            AdapterLog adapterLog,
            PaTagMap paTagMap,
            ExistingItemUnPublisher existingItemUnPublisher
    ) {
        this.contentResolver = checkNotNull(contentResolver);
        this.adapterLog = checkNotNull(adapterLog);
        this.paTagMap = checkNotNull(paTagMap);
        this.existingItemUnPublisher = checkNotNull(existingItemUnPublisher);
    }

    public static PaProgrammeProcessor create(
            ContentResolver contentResolver,
            AdapterLog adapterLog,
            PaTagMap paTagMap,
            ExistingItemUnPublisher existingItemUnPublisher
    ) {
        return new PaProgrammeProcessor(
                contentResolver,
                adapterLog,
                paTagMap,
                existingItemUnPublisher
        );
    }

    @Override
    public Optional<ContentHierarchyAndSummaries> process(
            ProgData progData,
            Channel channel,
            DateTimeZone zone,
            Timestamp updatedAt
    ) {
        try {
            log.trace("Channel: {} ProgData: {} UpdatedAt: {}", channel, progData, updatedAt);
            if (shouldNotProcess(progData)) {
                return Optional.absent();
            }

            Optional<Brand> possibleBrand = getBrand(progData, channel, updatedAt);
            Brand brandSummary = null;
            if (possibleBrand.isPresent() && hasBrandSummary(progData)) {
                brandSummary = getBrandSummary(progData, possibleBrand.get(), updatedAt);
            }

            Optional<Series> possibleSeries = getSeries(progData, channel, updatedAt);
            Series seriesSummary = null;
            if (possibleSeries.isPresent() && hasSeriesSummary(progData)) {
                seriesSummary = getSeriesSummary(progData, possibleSeries.get(), updatedAt);
            }

            boolean isEpisode = possibleBrand.isPresent() || possibleSeries.isPresent();

            ItemAndBroadcast itemAndBroadcast =
                    isClosedBrand(possibleBrand)
                    ? getClosedEpisode(progData, channel, zone, updatedAt)
                    : getFilmOrEpisode(progData, channel, zone, isEpisode, updatedAt);

            Item item = itemAndBroadcast.getItem();

            // TODO: there is an unknown bug preventing this from working (MBST-17174)
            if (!isEpisode) {
                item.setParentRef(null);
            }

            item.setGenericDescription(isGenericDescription(progData));
            item.addAlias(PaHelper.getProgIdAlias(progData.getProgId()));
            item.setLastUpdated(updatedAt.toDateTimeUTC());

            return Optional.of(new ContentHierarchyAndSummaries(
                    possibleBrand,
                    possibleSeries,
                    item,
                    itemAndBroadcast.getBroadcast().requireValue(),
                    Optional.fromNullable(brandSummary),
                    Optional.fromNullable(seriesSummary)
            ));
        } catch (Exception e) {
            log.error("Failed to process PA programme data", e);
            adapterLog.record(new AdapterLogEntry(Severity.ERROR)
                    .withCause(e)
                    .withSource(PaProgrammeProcessor.class)
                    .withDescription(e.getMessage())
            );
        }
        return Optional.absent();
    }

    @Override
    public Optional<ContentHierarchyWithoutBroadcast> process(
            ProgData progData,
            DateTimeZone zone,
            Timestamp updatedAt
    ) {
        try {
            if (shouldNotProcess(progData)) {
                return Optional.absent();
            }

            Optional<Brand> possibleBrand = getBrandWithoutChannel(progData, updatedAt);
            Brand brandSummary = null;
            if (possibleBrand.isPresent() && hasBrandSummary(progData)) {
                brandSummary = getBrandSummary(progData, possibleBrand.get(), updatedAt);
            }

            Optional<Series> possibleSeries = getSeriesWithoutChannel(progData, updatedAt);
            Series seriesSummary = null;
            if (possibleSeries.isPresent() && hasSeriesSummary(progData)) {
                seriesSummary = getSeriesSummary(progData, possibleSeries.get(), updatedAt);
            }

            boolean isEpisode = possibleBrand.isPresent() || possibleSeries.isPresent();

            Item item = getFilmOrEpisodeWithoutBroadcasts(
                    progData, isEpisode
            );

            // TODO: There is an unknown bug preventing this from working (MBST-17174)
            if (!isEpisode) {
                item.setParentRef(null);
            }

            item.setGenericDescription(isGenericDescription(progData));
            item.addAlias(PaHelper.getProgIdAlias(progData.getProgId()));
            item.setLastUpdated(updatedAt.toDateTimeUTC());

            return Optional.of(new ContentHierarchyWithoutBroadcast(
                    possibleBrand,
                    possibleSeries,
                    item,
                    Optional.fromNullable(brandSummary),
                    Optional.fromNullable(seriesSummary)
            ));
        } catch (Exception e) {
            log.warn("Failed to process PA programme data update", e);
            adapterLog.record(new AdapterLogEntry(Severity.ERROR)
                    .withCause(e)
                    .withSource(PaProgrammeProcessor.class)
                    .withDescription(e.getMessage())
            );
        }
        return Optional.absent();
    }

    private boolean shouldNotProcess(ProgData progData) {
        return !Strings.isNullOrEmpty(progData.getSeriesId())
                && IGNORED_BRANDS.contains(progData.getSeriesId());
    }

    private Boolean isGenericDescription(ProgData progData) {
        String generic = progData.getGeneric();
        if ("1".equals(generic)) {
            return true;
        }
        return null;
    }

    private Brand getBrandSummary(ProgData progData, Brand brand, Timestamp updatedAt) {
        String uri = brand.getCanonicalUri().replace(
                Publisher.PA.key(),
                Publisher.PA_SERIES_SUMMARIES.key()
        );
        Maybe<Identified> maybeBrandSummary = contentResolver.findByCanonicalUris(
                ImmutableList.of(uri)
        )
                .getFirstValue();
        Brand brandSummary;

        if (maybeBrandSummary.isNothing()) {
            brandSummary = new Brand();
            brandSummary.setCanonicalUri(uri);
            brandSummary.setPublisher(Publisher.PA_SERIES_SUMMARIES);
            brandSummary.setEquivalentTo(ImmutableSet.of(LookupRef.from(brand)));
        } else {
            brandSummary = (Brand) maybeBrandSummary.requireValue();
        }

        brandSummary.setLongDescription(progData.getSeriesSummary());
        brandSummary.setLastUpdated(updatedAt.toDateTimeUTC());

        return brandSummary;
    }

    private Series getSeriesSummary(ProgData progData, Series series, Timestamp updatedAt) {
        String uri = series.getCanonicalUri().replace(
                Publisher.PA.key(),
                Publisher.PA_SERIES_SUMMARIES.key()
        );
        Maybe<Identified> maybeSeriesSummary = contentResolver.findByCanonicalUris(
                ImmutableList.of(uri)
        )
                .getFirstValue();
        Series seriesSummary;

        if (maybeSeriesSummary.isNothing()) {
            seriesSummary = new Series();
            seriesSummary.setCanonicalUri(uri);
            seriesSummary.setPublisher(Publisher.PA_SERIES_SUMMARIES);
            seriesSummary.setEquivalentTo(ImmutableSet.of(LookupRef.from(series)));
        } else {
            seriesSummary = (Series) maybeSeriesSummary.requireValue();
        }

        seriesSummary.setLongDescription(progData.getSeason().getSeasonSummary());
        seriesSummary.setLastUpdated(updatedAt.toDateTimeUTC());

        return seriesSummary;
    }

    private boolean hasSeriesSummary(ProgData progData) {
        return (progData.getSeason() != null
                && !Strings.isNullOrEmpty(progData.getSeason().getSeasonSummary()));
    }

    private boolean hasBrandSummary(ProgData progData) {
        return !Strings.isNullOrEmpty(progData.getSeriesSummary());
    }

    private boolean isClosedBrand(Optional<Brand> brand) {
        return brand.isPresent() && CLOSED_BRAND.equals(brand.get().getCanonicalUri());
    }

    private ItemAndBroadcast getClosedEpisode(
            ProgData progData, Channel channel,
            DateTimeZone zone, Timestamp updatedAt
    ) {
        String uri = CLOSED_EPISODE_PREFIX + getClosedPostfix(channel);

        java.util.Optional<Identified> existing = contentResolver.findByCanonicalUris(
                ImmutableList.of(uri)
        )
                .getFirstValue()
                .toOptional();

        Episode episode;

        if (existing.isPresent() && existing.get() instanceof Episode) {
            episode = (Episode) existing.get();
        } else {
            episode = (Episode) getBasicEpisode(
                    progData,
                    true,
                    uri
            );
        }
        episode.setCurie(CLOSED_CURIE + getClosedPostfix(channel));
        episode.setTitle(progData.getTitle());
        episode.setScheduleOnly(true);
        episode.setMediaType(channel.getMediaType());

        Version version = findBestVersion(episode.getVersions());

        Broadcast broadcast = broadcast(progData, channel, zone, updatedAt);
        addBroadcast(version, broadcast);

        return new ItemAndBroadcast(episode, Maybe.just(broadcast));

    }

    @SuppressWarnings("deprecation")
    private String getClosedPostfix(Channel channel) {
        return "_" + channel.getKey();
    }

    private Optional<Brand> getBrand(ProgData progData, Channel channel, Timestamp updatedAt) {

        Optional<Brand> possibleBrand = getBrandWithoutChannel(progData, updatedAt);
        if (possibleBrand.isPresent()) {
            Brand brand = possibleBrand.get();
            brand.setSpecialization(specialization(progData, channel));
            brand.setMediaType(channel.getMediaType());
            setImages(
                    progData.getPictures(),
                    brand,
                    PA_PICTURE_TYPE_BRAND,
                    PA_PICTURE_TYPE_SERIES,
                    Maybe.<String>nothing()
            );
        }

        return possibleBrand;
    }

    private Optional<Brand> getBrandWithoutChannel(ProgData progData, Timestamp updatedAt) {

        String brandId = progData.getSeriesId();
        if (Strings.isNullOrEmpty(brandId) || Strings.isNullOrEmpty(brandId.trim())) {
            return Optional.absent();
        }

        String brandUri = PaHelper.getBrandUri(brandId);
        Alias brandAlias = PaHelper.getBrandAlias(brandId);

        Maybe<Identified> possiblePrevious = contentResolver.findByCanonicalUris(
                ImmutableList.of(brandUri)
        )
                .getFirstValue();

        Brand brand = possiblePrevious.hasValue()
                      ? (Brand) possiblePrevious.requireValue()
                      : new Brand(brandUri, "pa:b-" + brandId, Publisher.PA);

        brand.addAlias(brandAlias);
        brand.setTitle(progData.getTitle());
        brand.setDescription(Strings.emptyToNull(progData.getSeriesSynopsis()));
        setCertificate(progData, brand);
        setGenres(progData, brand);
        setTopicRefs(brand);

        if (isClosedBrand(Optional.of(brand))) {
            brand.setScheduleOnly(true);
        }

        brand.setLastUpdated(updatedAt.toDateTimeUTC());

        return Optional.of(brand);
    }

    /**
     * If pictures is not null, add a list of images of the given primary type to the described
     * object. If no images of the primary type are found, fall back to the first, or optional
     * second type. Only one (the first) fallback image will be used. Ordering is preserved for
     * images. If pictures is null, this method does nothing.
     *
     * @param pictures           The picture collection to use
     * @param described          The object that will have images added
     * @param primaryImageType   The primary image type to add
     * @param firstFallbackType  The preferred fallback image type to add
     * @param secondFallbackType The optional fallback image type to add
     */
    void setImages(
            Pictures pictures,
            Described described,
            String primaryImageType,
            String firstFallbackType,
            Maybe<String> secondFallbackType
    ) {
        if (pictures != null) {
            Set<Image> images = Sets.newLinkedHashSet();
            Image fallbackImage = null;
            boolean hasFirstFallbackType = false;

            for (PictureUsage picture : pictures.getPictureUsage()) {
                Image image = createImage(picture);

                if (secondFallbackType.hasValue() &&
                        picture.getType().equals(secondFallbackType.requireValue()) &&
                        images.isEmpty() &&
                        fallbackImage == null) {
                    setPrimaryImage(described, image, picture);
                    fallbackImage = image;
                }
                if (picture.getType().equals(firstFallbackType)
                        && images.isEmpty()
                        && !hasFirstFallbackType) {
                    setPrimaryImage(described, image, picture);
                    fallbackImage = image;
                    hasFirstFallbackType = true;
                }
                if (picture.getType().equals(primaryImageType)) {
                    if (images.size() == 0) {
                        setPrimaryImage(described, image, picture);
                    }
                    images.add(image);
                }
            }

            if (!images.isEmpty()) {
                described.setImages(images);
            } else if (fallbackImage != null) {
                described.setImages(ImmutableSet.of(fallbackImage));
            }
        }
    }

    private void setPrimaryImage(Described described, Image image, PictureUsage picture) {
        image.setType(ImageType.PRIMARY);
        // The image URL is set to the "legacy" URL of http://images.../pa/image.jpg since there
        // are external dependencies on it. The new image block moves to the new URL scheme of
        // http://images.../pressassociation.com/image.jpg
        described.setImage(IMAGE_URL_BASE + picture.getvalue());
    }

    private Image createImage(PictureUsage pictureUsage) {
        String imageUri = IMAGE_URL_BASE + pictureUsage.getvalue();
        Image image = new Image(imageUri);

        image.setHeight(576);
        image.setWidth(1024);
        image.setType(ImageType.ADDITIONAL);
        image.setAspectRatio(ImageAspectRatio.SIXTEEN_BY_NINE);
        image.setMimeType(MimeType.IMAGE_JPG);
        image.setAvailabilityStart(fromPaDate(pictureUsage.getStartDate()));
        DateTime expiry = fromPaDate(pictureUsage.getExpiryDate());
        if (expiry != null) {
            image.setAvailabilityEnd(expiry.plusDays(1));
        } else {
            image.setAvailabilityEnd(null);
        }
        return image;
    }

    private Optional<Series> getSeries(ProgData progData, Channel channel, Timestamp updatedAt) {
        Optional<Series> possibleSeries = getSeriesWithoutChannel(progData, updatedAt);

        if (possibleSeries.isPresent()) {
            Series series = possibleSeries.get();
            series.setSpecialization(specialization(progData, channel));
            setImages(
                    progData.getPictures(),
                    series,
                    PA_PICTURE_TYPE_SERIES,
                    PA_PICTURE_TYPE_BRAND,
                    Maybe.<String>nothing()
            );
        }
        return possibleSeries;
    }

    private Optional<Series> getSeriesWithoutChannel(ProgData progData, Timestamp updatedAt) {
        if (Strings.isNullOrEmpty(progData.getSeriesNumber())
                || Strings.isNullOrEmpty(progData.getSeriesId())) {
            return Optional.absent();
        }
        String seriesUri = PaHelper.getSeriesUri(
                progData.getSeriesId(),
                progData.getSeriesNumber()
        );

        Alias seriesAlias;
        // pa series <=> to atlas brand, so we need season.id NOT series.id. Related to ENG-979
        if (progData.getSeason() != null && !Strings.isNullOrEmpty(progData.getSeason().getId())) {
            seriesAlias = PaHelper.getSeriesAlias(progData.getSeason().getId());
        }
        else {
            seriesAlias = PaHelper.getSeriesAlias(
                    progData.getSeriesId(),     // this is actually the brand id?
                    progData.getSeriesNumber()
            );
        }

        Maybe<Identified> possiblePrevious = contentResolver.findByCanonicalUris(
                ImmutableList.of(seriesUri)
        )
                .getFirstValue();

        Series series = possiblePrevious.hasValue()
                        ? (Series) possiblePrevious.requireValue()
                        : new Series(
                                seriesUri,
                                "pa:s-" + progData.getSeriesId()
                                        + "-" + progData.getSeriesNumber(),
                                Publisher.PA
                        );

        series.addAlias(seriesAlias);

        if (progData.getEpisodeTotal() != null && progData.getEpisodeTotal().trim().length() > 0) {
            try {
                series.setTotalEpisodes(Integer.parseInt(progData.getEpisodeTotal().trim()));
            } catch (NumberFormatException e) {
                adapterLog.record(warnEntry().withCause(e)
                        .withSource(getClass())
                        .withDescription(
                                "Couldn't parse episode_total %s",
                                progData.getEpisodeTotal().trim()
                        ));
            }
        }

        if (progData.getSeriesNumber() != null && progData.getSeriesNumber().trim().length() > 0) {
            try {
                series.withSeriesNumber(Integer.parseInt(progData.getSeriesNumber().trim()));
            } catch (NumberFormatException e) {
                adapterLog.record(warnEntry().withCause(e)
                        .withSource(getClass())
                        .withDescription(
                                "Couldn't parse series_number %s",
                                progData.getSeriesNumber().trim()
                        ));
            }
        }

        series.setPublisher(Publisher.PA);
        setCertificate(progData, series);
        setGenres(progData, series);
        setTopicRefs(series);

        series.setLastUpdated(updatedAt.toDateTimeUTC());

        return Optional.of(series);
    }

    private DateTime fromPaDate(String paDate) {
        if (Strings.isNullOrEmpty(paDate)) {
            return null;
        }
        return PA_DATE_FORMAT.parseDateTime(paDate).withZone(DateTimeZone.UTC);
    }

    private ItemAndBroadcast getFilmOrEpisode(
            ProgData progData,
            Channel channel,
            DateTimeZone zone,
            boolean isEpisode,
            Timestamp updatedAt
    ) {
        return specialization(progData, channel) == Specialization.FILM
               ? getFilm(progData, channel, zone, updatedAt)
               : getEpisode(progData, channel, zone, isEpisode, updatedAt);
    }

    private Item getFilmOrEpisodeWithoutBroadcasts(
            ProgData progData,
            boolean isEpisode
    ) {
        return getBooleanValue(progData.getAttr().getFilm())
               ? getFilmWithoutBroadcast(progData)
               : getEpisodeWithoutBroadcast(progData, isEpisode);
    }

    private ItemAndBroadcast getFilm(
            ProgData progData,
            Channel channel,
            DateTimeZone zone,
            Timestamp updatedAt
    ) {
        Item film = getBasicFilmWithoutBroadcast(progData);

        setCommonDetails(progData, film, Optional.of(channel));
        Broadcast broadcast = addBroadcast(progData, film, channel, zone, updatedAt);

        setImages(
                progData.getPictures(), film, PA_PICTURE_TYPE_EPISODE,
                PA_PICTURE_TYPE_SERIES, Maybe.just(PA_PICTURE_TYPE_BRAND)
        );

        return new ItemAndBroadcast(film, Maybe.just(broadcast));
    }

    private Item getFilmWithoutBroadcast(ProgData progData) {
        Item film = getBasicFilmWithoutBroadcast(progData);
        setCommonDetails(progData, film, Optional.<Channel>absent());

        return film;
    }

    private Item getBasicFilmWithoutBroadcast(ProgData progData) {
        // Previously when constructing the uri we would failover to ProgId if the rtFilmIdentifier
        // was missing but still write to the same namespace. To avoid creating duplicates we have
        // to un-publish any existing content with these old identifiers
        existingItemUnPublisher.unPublishItems(
                PaHelper.getLegacyFilmUri(progData.getProgId())
        );
        Optional<String> rtFilmIdentifier = rtFilmIdentifierFor(progData);
        if (rtFilmIdentifier.isPresent()) {
            existingItemUnPublisher.unPublishItems(
                    PaHelper.getLegacyFilmUri(rtFilmIdentifier.get())
            );
        }

        String uri = PaHelper.getFilmUri(progData.getProgId());

        Film film = getFilmFromResolvedContent(
                progData,
                contentResolver.findByCanonicalUris(
                        ImmutableList.of(uri)
                )
                        .getFirstValue()
                        .toOptional(),
                uri
        );

        if (progData.getFilmYear() != null
                && MoreStrings.containsOnlyAsciiDigits(progData.getFilmYear())) {
            film.setYear(Integer.parseInt(progData.getFilmYear()));
        }
        return film;
    }

    private Film getFilmFromResolvedContent(
            ProgData progData,
            java.util.Optional<Identified> resolvedContent,
            String uri
    ) {
        if (!resolvedContent.isPresent()) {
            return getBasicFilm(progData, uri);
        }

        Identified previous = resolvedContent.get();

        Film film;
        if (previous instanceof Film) {
            film = (Film) previous;
        } else {
            film = new Film();
            Content.copyTo((Content) previous, film);
        }
        return film;
    }

    private void setCommonDetails(ProgData progData, Item episode, Optional<Channel> channel) {
        // Currently Welsh channels have Welsh titles/descriptions which flip the English ones,
        // resulting in many writes. We'll only take the Welsh title if we don't
        // already have a title from another channel
        boolean isWelshChannel = channel.isPresent() && channel.get().getUri().contains("wales");

        if (episode.getTitle() == null || !isWelshChannel) {
            if (progData.getEpisodeTitle() != null) {
                episode.setTitle(progData.getEpisodeTitle());
            } else {
                episode.setTitle(progData.getTitle());
            }
        }

        setDescription(progData, episode, isWelshChannel);

        if (channel.isPresent()) {
            episode.setMediaType(channel.get().getMediaType());
            episode.setSpecialization(specialization(progData, channel.get()));
        }

        setGenres(progData, episode);
        setTopicRefs(episode);

        if (progData.getCountry() != null) {
            episode.setCountriesOfOrigin(countryMap.parseCountries(progData.getCountry()));
        }

        if (progData.getAttr() != null) {
            episode.setBlackAndWhite(getBooleanValue(progData.getAttr().getBw()));
        }

        episode.setPeople(people(progData));

        setCertificate(progData, episode);

        // Adding a film alias only used to happen when type was film. Previously this was
        // decided based on the existence of the field rt_filmnumber. That's faulty, though,
        // so we changed it to use the film flag. In order to maintain backward-compatibilty
        // and only set the film alias when a rt_filmnumber value exists, we make that
        // check here.

        Optional<String> rtFilmIdentifier = rtFilmIdentifierFor(progData);
        if (rtFilmIdentifier.isPresent()) {
            episode.addAliasUrl(PaHelper.getLegacyFilmUri(rtFilmIdentifier.get()));
            episode.addAlias(PaHelper.getLegacyFilmAlias(rtFilmIdentifier.get()));
        }
        episode.addAliasUrl(PaHelper.getAlias(progData.getProgId()));
        episode.addAlias(PaHelper.getNewFilmAlias(progData.getProgId()));
    }

    private Broadcast addBroadcast(
            ProgData progData, Item item, Channel channel,
            DateTimeZone zone, Timestamp updatedAt
    ) {
        Version version = findBestVersion(item.getVersions());
        version.set3d(getBooleanValue(progData.getAttr().getThreeD()));
        version.setDuration(Duration.standardMinutes(Long.valueOf(progData.getDuration())));

        Broadcast broadcast = broadcast(progData, channel, zone, updatedAt);
        addBroadcast(version, broadcast);

        return broadcast;
    }

    private void setDescription(ProgData progData, Item item, boolean isWelshChannel) {
        if (progData.getBillings() != null && progData.getAttr().getContinued().equals("no")) {
            for (Billing billing : progData.getBillings().getBilling()) {

                if ((item.getDescription() == null || !isWelshChannel)
                        && billing.getType().equals(BILLING_DESCRIPTION)) {
                    item.setDescription(billing.getvalue());
                }
                if ((item.getShortDescription() == null || !isWelshChannel)
                        && billing.getType().equals(BILLING_SHORT_DESCRIPTION)) {
                    item.setShortDescription(billing.getvalue());
                }
                if ((item.getMediumDescription() == null || !isWelshChannel)
                        && billing.getType().equals(BILLING_MEDIUM_DESCRIPTION)) {
                    item.setMediumDescription(billing.getvalue());
                }
                if ((item.getLongDescription() == null || !isWelshChannel)
                        && billing.getType().equals(BILLING_LONG_DESCRIPTION)) {
                    item.setLongDescription(billing.getvalue());
                }
            }
        }
    }

    private void setCertificate(ProgData progData, Content content) {
        if (progData.getCertificate() != null) {
            content.setCertificates(ImmutableList.of(new Certificate(
                    progData.getCertificate(),
                    Countries.GB
            )));
        }
    }

    private void setGenres(ProgData progData, Content content) {
        Set<String> extractedGenres = genreMap.map(ImmutableSet.copyOf(Iterables.transform(
                progData.getCategory(),
                category -> "http://pressassociation.com/genres/" + category.getCategoryCode()
        )));
        extractedGenres.remove("http://pressassociation.com/genres/BE00");
        if (!extractedGenres.isEmpty()) {
            content.setGenres(extractedGenres);
        }
    }

    private ItemAndBroadcast getEpisode(
            ProgData progData,
            Channel channel,
            DateTimeZone zone,
            boolean isEpisode,
            Timestamp updatedAt
    ) {
        Item episode = getBasicEpisodeWithoutBroadcast(progData, isEpisode);

        setCommonDetails(progData, episode, Optional.of(channel));
        Broadcast broadcast = addBroadcast(progData, episode, channel, zone, updatedAt);

        setImages(
                progData.getPictures(), episode, PA_PICTURE_TYPE_EPISODE,
                PA_PICTURE_TYPE_SERIES, Maybe.just(PA_PICTURE_TYPE_BRAND)
        );

        return new ItemAndBroadcast(episode, Maybe.just(broadcast));
    }

    private Item getEpisodeWithoutBroadcast(ProgData progData, boolean isEpisode) {
        Item episode = getBasicEpisodeWithoutBroadcast(progData, isEpisode);
        setCommonDetails(progData, episode, Optional.<Channel>absent());

        return episode;
    }

    private Item getBasicEpisodeWithoutBroadcast(ProgData progData, boolean isEpisode) {
        // We used to write films with a uri schema that is being sunsetted. We un-publish
        // any film with the legacy uri here in case this episode was originally ingested as a film
        // with that legacy uri.
        existingItemUnPublisher.unPublishItems(
                PaHelper.getLegacyFilmUri(progData.getProgId())
        );

        String uri = PaHelper.getEpisodeUri(progData.getProgId());

        java.util.Optional<Identified> existing = contentResolver.findByCanonicalUris(
                ImmutableList.of(uri)
        )
                .getFirstValue()
                .toOptional();

        Item item;

        if (existing.isPresent()) {
            Item previous = (Item) existing.get();

            if (!(previous instanceof Episode) && isEpisode) {
                String message = String.format(
                        "%s resolved as %s being ingested as Episode",
                        uri, previous.getClass().getSimpleName()
                );

                adapterLog.record(warnEntry()
                        .withSource(getClass())
                        .withDescription(message)
                );
                log.info(message);

                item = convertItemToEpisode(previous);
            } else if (previous instanceof Episode && !isEpisode) {
                String message = String.format(
                        "%s resolved as %s being ingested as Item",
                        uri, previous.getClass().getSimpleName()
                );

                adapterLog.record(errorEntry()
                        .withSource(getClass())
                        .withDescription(message)
                );
                log.info(message);

                item = new Item();
                Item.copyTo(previous, item);
            } else {
                item = previous;
            }
        } else {
            item = getBasicEpisode(progData, isEpisode, uri);
        }

        if (SCHEDULED_ONLY_EPISODE.equals(uri)) {
            item.setScheduleOnly(true);
        }

        item.addAlias(PaHelper.getEpisodeAlias(progData.getProgId()));

        try {
            if (item instanceof Episode) {
                Episode episode = (Episode) item;
                episode.setSpecial(getBooleanValue(progData.getAttr().getSpecial()));
                episode.setEpisodeNumber(episodeNumber(progData));
                episode.setSeriesNumber(seriesNumber(progData));
            }
        } catch (NumberFormatException e) {
            // sometimes we don't get valid numbers
            log.warn("Failed to parse a numeric field for PA episode {}", uri, e);
        }
        return item;
    }

    private Integer seriesNumber(ProgData progData) {
        String seriesNumber = progData.getSeriesNumber();
        if (seriesNumber != null) {
            return Integer.valueOf(seriesNumber);
        }
        return null;
    }

    private Integer episodeNumber(ProgData progData) {
        String episodeNumber = progData.getEpisodeNumber();
        if (episodeNumber != null) {
            return Integer.valueOf(episodeNumber);
        }
        return null;
    }

    private Item convertItemToEpisode(Item item) {
        Episode episode = new Episode(item.getCanonicalUri(), item.getCurie(), item.getPublisher());
        episode.setAliases(item.getAliases());
        episode.setAliasUrls(item.getAliasUrls());
        episode.setBlackAndWhite(item.getBlackAndWhite());
        episode.setClips(item.getClips());
        episode.setParentRef(item.getContainer());
        episode.setCountriesOfOrigin(item.getCountriesOfOrigin());
        episode.setDescription(item.getDescription());
        episode.setFirstSeen(item.getFirstSeen());
        episode.setGenres(item.getGenres());
        episode.setImage(item.getImage());
        episode.setImages(item.getImages());
        episode.setIsLongForm(item.getIsLongForm());
        episode.setLastFetched(item.getLastFetched());
        episode.setLastUpdated(item.getLastUpdated());
        episode.setMediaType(item.getMediaType());
        episode.setPeople(item.getPeople());
        episode.setScheduleOnly(item.isScheduleOnly());
        episode.setSpecialization(item.getSpecialization());
        episode.setTags(item.getTags());
        episode.setThisOrChildLastUpdated(item.getThisOrChildLastUpdated());
        episode.setThumbnail(item.getThumbnail());
        episode.setTitle(item.getTitle());
        episode.setVersions(item.getVersions());
        return episode;
    }

    private Broadcast broadcast(
            ProgData progData,
            Channel channel,
            DateTimeZone zone,
            Timestamp updateAt
    ) {
        Duration duration = Duration.standardMinutes(Long.valueOf(progData.getDuration()));

        DateTime transmissionTime = getTransmissionTime(
                progData.getDate(),
                progData.getTime(),
                zone
        );

        Broadcast broadcast = new Broadcast(channel.getUri(), transmissionTime, duration).withId(
                PaHelper.getBroadcastId(progData.getShowingId()));

        if (progData.getAttr() != null) {
            broadcast.setRepeat(getBooleanValue(progData.getAttr().getRepeat()));
            broadcast.setRevisedRepeat(getBooleanValue(progData.getAttr().getRevisedRepeat()));
            broadcast.setSubtitled(getBooleanValue(progData.getAttr().getSubtitles()));
            broadcast.setSigned(getBooleanValue(progData.getAttr().getSignLang()));
            broadcast.setAudioDescribed(getBooleanValue(progData.getAttr().getAudioDes()));
            broadcast.setHighDefinition(getBooleanValue(progData.getAttr().getHd()));
            broadcast.setWidescreen(getBooleanValue(progData.getAttr().getWidescreen()));
            broadcast.setLive(getBooleanValue(progData.getAttr().getLive()));
            broadcast.setSurround(getBooleanValue(progData.getAttr().getSurround()));
            broadcast.setPremiere(getBooleanValue(progData.getAttr().getPremiere()));

            Boolean newSeries = getBooleanValue(progData.getAttr().getNewSeries());
            Boolean newEpisode = getBooleanValue(progData.getAttr().getNewEpisode());

            broadcast.setNewSeries(newSeries);
            broadcast.setNewEpisode(newEpisode);
            broadcast.setNewOneOff(getBooleanValue(progData.getAttr().getNewOneOff()));
            broadcast.setContinuation(getBooleanValue(progData.getAttr().getContinued()));
        }
        broadcast.setLastUpdated(updateAt.toDateTimeUTC());
        return broadcast;
    }

    private void addBroadcast(Version version, Broadcast newBroadcast) {
        if (Strings.isNullOrEmpty(newBroadcast.getSourceId())) {
            return;
        }

        Set<Broadcast> broadcasts = Sets.newHashSet();
        Interval newBroadcastInterval = newBroadcast.transmissionInterval().requireValue();

        for (Broadcast existingBroadcast : version.getBroadcasts()) {
            if (newBroadcast.getSourceId().equals(existingBroadcast.getSourceId())) {
                continue;
            }
            if (existingBroadcast.transmissionInterval().hasValue()) {
                Interval currentInterval = existingBroadcast.transmissionInterval().requireValue();
                if (existingBroadcast.getBroadcastOn().equals(newBroadcast.getBroadcastOn())
                        && currentInterval.overlaps(newBroadcastInterval)) {
                    continue;
                }
            }
            broadcasts.add(existingBroadcast);
        }
        broadcasts.add(newBroadcast);

        version.setBroadcasts(broadcasts);
    }

    private List<CrewMember> people(ProgData progData) {
        List<CrewMember> people = Lists.newArrayList();

        for (CastMember cast : progData.getCastMember()) {
            if (!Strings.isNullOrEmpty(cast.getActor().getPersonId())) {
                Actor actor = Actor.actor(
                        cast.getActor().getPersonId(),
                        cast.getActor().getvalue(),
                        cast.getCharacter(),
                        Publisher.PA
                );
                if (!people.contains(actor)) {
                    people.add(actor);
                }
            }
        }

        for (StaffMember staffMember : progData.getStaffMember()) {
            if (!Strings.isNullOrEmpty(staffMember.getPerson().getPersonId())) {
                String roleKey = staffMember.getRole().toLowerCase().replace(' ', '_');
                CrewMember crewMember = CrewMember.crewMember(
                        staffMember.getPerson().getPersonId(),
                        staffMember.getPerson().getvalue(),
                        roleKey,
                        Publisher.PA
                );
                if (!people.contains(crewMember)) {
                    people.add(crewMember);
                }
            }
        }

        return people;
    }

    private Version findBestVersion(Iterable<Version> versions) {
        for (Version version : versions) {
            if (version.getProvider() == Publisher.PA) {
                return version;
            }
        }

        return versions.iterator().next();
    }

    private Film getBasicFilm(ProgData progData, String uri) {
        Film film = new Film(
                uri,
                PaHelper.getEpisodeCurie(progData.getProgId()),
                Publisher.PA
        );
        setBasicDetails(progData, film);

        return film;
    }

    private Item getBasicEpisode(ProgData progData, boolean isEpisode, String uri) {
        Item item = isEpisode ? new Episode() : new Item();
        item.setCanonicalUri(uri);
        item.setCurie("pa:e-" + progData.getProgId());
        item.setPublisher(Publisher.PA);
        setBasicDetails(progData, item);
        return item;
    }

    private void setBasicDetails(ProgData progData, Item item) {
        setCertificate(progData, item);
        Version version = new Version();
        version.setProvider(Publisher.PA);
        version.set3d(getBooleanValue(progData.getAttr().getThreeD()));
        item.addVersion(version);

        if (progData.getDuration() != null) {

            Duration duration = Duration.standardMinutes(Long.valueOf(progData.getDuration()));
            version.setDuration(duration);
        }

        item.addVersion(version);
    }

    protected Specialization specialization(ProgData progData, Channel channel) {
        if (MediaType.AUDIO.equals(channel.getMediaType())) {
            return Specialization.RADIO;
        }
        return getBooleanValue(progData.getAttr().getFilm())
               ? Specialization.FILM
               : Specialization.TV;
    }

    protected static DateTime getTransmissionTime(String date, String time, DateTimeZone zone) {
        String dateString = date + "-" + time;
        return DateTimeFormat.forPattern("dd/MM/yyyy-HH:mm")
                .withZone(zone)
                .parseDateTime(dateString);
    }

    private Optional<String> rtFilmIdentifierFor(ProgData progData) {
        return Optional.fromNullable(Strings.emptyToNull(progData.getRtFilmnumber()));
    }

    private static Boolean getBooleanValue(String value) {
        if (value != null) {
            return value.equalsIgnoreCase(YES);
        }
        return null;
    }

    private void setTopicRefs(Content content) {
        Set<TopicRef> tagsFromGenres = paTagMap.mapGenresToTopicRefs(content.getGenres());
        content.setTopicRefs(tagsFromGenres);
    }
}

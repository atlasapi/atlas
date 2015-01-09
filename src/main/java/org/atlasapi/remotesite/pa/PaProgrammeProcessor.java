package org.atlasapi.remotesite.pa;

import static org.atlasapi.persistence.logging.AdapterLogEntry.errorEntry;
import static org.atlasapi.persistence.logging.AdapterLogEntry.warnEntry;

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
import org.atlasapi.media.entity.Version;
import org.atlasapi.media.util.ItemAndBroadcast;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.logging.AdapterLog;
import org.atlasapi.persistence.logging.AdapterLogEntry;
import org.atlasapi.persistence.logging.AdapterLogEntry.Severity;
import org.atlasapi.remotesite.pa.listings.bindings.Attr;
import org.atlasapi.remotesite.pa.listings.bindings.Billing;
import org.atlasapi.remotesite.pa.listings.bindings.CastMember;
import org.atlasapi.remotesite.pa.listings.bindings.Category;
import org.atlasapi.remotesite.pa.listings.bindings.PictureUsage;
import org.atlasapi.remotesite.pa.listings.bindings.Pictures;
import org.atlasapi.remotesite.pa.listings.bindings.ProgData;
import org.atlasapi.remotesite.pa.listings.bindings.StaffMember;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.intl.Countries;
import com.metabroadcast.common.media.MimeType;
import com.metabroadcast.common.text.MoreStrings;
import com.metabroadcast.common.time.Timestamp;

public class PaProgrammeProcessor implements PaProgDataProcessor {
    
    static final String PA_PICTURE_TYPE_EPISODE = "episode";
    static final String PA_PICTURE_TYPE_BRAND   = "series";  // Counter-intuitively PA use 'series' where we use 'brand'
    static final String PA_PICTURE_TYPE_SERIES  = "season";  // .. and 'season' where we use 'series'
    
    static final String IMAGE_URL_BASE = "http://images.atlas.metabroadcast.com/pressassociation.com/";
    public static final String BROADCAST_ID_PREFIX = "pa:";
    
    private static final DateTimeFormatter PA_DATE_FORMAT = DateTimeFormat.forPattern("dd/MM/yyyy");
    
    private static final String YES = "yes";
    private static final String CLOSED_BRAND = "http://pressassociation.com/brands/8267";
    private static final String CLOSED_EPISODE = "http://pressassociation.com/episodes/closed";
    private static final String CLOSED_CURIE = "pa:closed";    
    
    private static final List<String> IGNORED_BRANDS = ImmutableList.of("70214", "84575");    // 70214 is 'TBA' brand, 84575 is 'Film TBA'
    
    private final ContentResolver contentResolver;
    private final AdapterLog log;
    private final PaCountryMap countryMap = new PaCountryMap();
    
    private final GenreMap genreMap = new PaGenreMap();

    public PaProgrammeProcessor(ContentWriter contentWriter, ContentResolver contentResolver, AdapterLog log) {
        this.contentResolver = contentResolver;
        this.log = log;
    }

    @Override
    public ContentHierarchyAndSummaries process(ProgData progData, Channel channel, DateTimeZone zone, Timestamp updatedAt) {
        try {
            if (! Strings.isNullOrEmpty(progData.getSeriesId()) && IGNORED_BRANDS.contains(progData.getSeriesId())) {
                return null;
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

            ItemAndBroadcast itemAndBroadcast = isClosedBrand(possibleBrand) ? 
                   getClosedEpisode(possibleBrand.get(), progData, channel, zone, updatedAt) 
                   : getFilmOrEpisode(progData, channel, zone, possibleBrand.isPresent() || possibleSeries.isPresent(), updatedAt);

            
        	Item item = itemAndBroadcast.getItem();
        	item.setGenericDescription(isGenericDescription(progData));
            item.setLastUpdated(updatedAt.toDateTimeUTC());
            
            return new ContentHierarchyAndSummaries(possibleBrand, possibleSeries, item, itemAndBroadcast.getBroadcast().requireValue(), 
                    Optional.fromNullable(brandSummary), Optional.fromNullable(seriesSummary));
        } catch (Exception e) {
        	e.printStackTrace();
        	log.record(new AdapterLogEntry(Severity.ERROR).withCause(e).withSource(PaProgrammeProcessor.class).withDescription(e.getMessage()));
        }
        return null;
    }

    private Boolean isGenericDescription(ProgData progData) {
        String generic = progData.getGeneric();
        if ("1".equals(generic)) {
            return true;
        }
        return null;
    }

    private Brand getBrandSummary(ProgData progData, Brand brand, Timestamp updatedAt) {
        String uri = brand.getCanonicalUri().replace(Publisher.PA.key(), Publisher.PA_SERIES_SUMMARIES.key());
        Maybe<Identified> maybeBrandSummary = contentResolver.findByCanonicalUris(ImmutableList.of(uri)).getFirstValue();
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
        String uri = series.getCanonicalUri().replace(Publisher.PA.key(), Publisher.PA_SERIES_SUMMARIES.key());
        Maybe<Identified> maybeSeriesSummary = contentResolver.findByCanonicalUris(ImmutableList.of(uri)).getFirstValue();
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

    private ItemAndBroadcast getClosedEpisode(Brand brand, ProgData progData, Channel channel, DateTimeZone zone, Timestamp updatedAt) {
        String uri = CLOSED_EPISODE+getClosedPostfix(channel);
        Maybe<Identified> resolvedContent = contentResolver.findByCanonicalUris(ImmutableList.of(uri)).getFirstValue();

        Episode episode;
        if (resolvedContent.hasValue() && resolvedContent.requireValue() instanceof Episode) {
            episode = (Episode) resolvedContent.requireValue();
        } else {
            episode = (Episode) getBasicEpisode(progData, true);
        }
        episode.setCanonicalUri(uri);
        episode.setCurie(CLOSED_CURIE+getClosedPostfix(channel));
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
        return "_"+channel.getKey();
    }
    
    private Optional<Brand> getBrand(ProgData progData, Channel channel, Timestamp updatedAt) {
        String brandId = progData.getSeriesId();
        if (Strings.isNullOrEmpty(brandId) || Strings.isNullOrEmpty(brandId.trim())) {
            return Optional.absent();
        }

        String brandUri = PaHelper.getBrandUri(brandId);
        Alias brandAlias = PaHelper.getBrandAlias(brandId);
        
        Maybe<Identified> possiblePrevious = contentResolver.findByCanonicalUris(ImmutableList.of(brandUri)).getFirstValue();
        
        Brand brand = possiblePrevious.hasValue() ? (Brand) possiblePrevious.requireValue() : new Brand(brandUri, "pa:b-" + brandId, Publisher.PA);
        
        brand.addAlias(brandAlias);
        brand.setTitle(progData.getTitle());
        brand.setDescription(Strings.emptyToNull(progData.getSeriesSynopsis()));
        brand.setSpecialization(specialization(progData, channel));
        brand.setMediaType(channel.getMediaType());
        setCertificate(progData, brand);
        setGenres(progData, brand);

        selectImages(progData.getPictures(), brand, PA_PICTURE_TYPE_BRAND, PA_PICTURE_TYPE_SERIES, Maybe.<String>nothing());

        if (isClosedBrand(Optional.of(brand))) {
            brand.setScheduleOnly(true);
        }
        brand.setLastUpdated(updatedAt.toDateTimeUTC());

        return Optional.of(brand);
    }

    /**
     * If pictures is not null, add a list of images of the given primary type to the described object.
     * If no images of the primary type are found, fall back to the first, or optional second type. Only
     * one (the first) fallback image will be used. Ordering is preserved for images. If pictures is null,
     * this method does nothing.
     * @param pictures The picture collection to use
     * @param described The object that will have images added
     * @param primaryImageType The primary image type to add
     * @param firstFallbackType The preferred fallback image type to add
     * @param secondFallbackType The optional fallback image type to add
     */
    void selectImages(Pictures pictures, Described described, String primaryImageType, String firstFallbackType, Maybe<String> secondFallbackType) {
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
                if (picture.getType().equals(firstFallbackType) && images.isEmpty() && !hasFirstFallbackType) {
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
        
        if (Strings.isNullOrEmpty(progData.getSeriesNumber()) || Strings.isNullOrEmpty(progData.getSeriesId())) {
            return Optional.<Series>absent();
        }
        String seriesUri = PaHelper.getSeriesUri(progData.getSeriesId(), progData.getSeriesNumber());
        Alias seriesAlias = PaHelper.getSeriesAlias(progData.getSeriesId(), progData.getSeriesNumber());
        
        
        Maybe<Identified> possiblePrevious = contentResolver.findByCanonicalUris(ImmutableList.of(seriesUri)).getFirstValue();
        
        Series series = possiblePrevious.hasValue() ? (Series) possiblePrevious.requireValue() : new Series(seriesUri, "pa:s-" + progData.getSeriesId() + "-" + progData.getSeriesNumber(), Publisher.PA);
        
        series.addAlias(seriesAlias);
        
        if(progData.getEpisodeTotal() != null && progData.getEpisodeTotal().trim().length() > 0) {
            try {
                series.setTotalEpisodes(Integer.parseInt(progData.getEpisodeTotal().trim()));
            } catch (NumberFormatException e) {
                log.record(warnEntry().withCause(e).withSource(getClass()).withDescription("Couldn't parse episode_total %s", progData.getEpisodeTotal().trim()));
            }
        }
        
        if(progData.getSeriesNumber() != null && progData.getSeriesNumber().trim().length() > 0) {
            try {
                series.withSeriesNumber(Integer.parseInt(progData.getSeriesNumber().trim()));
            } catch (NumberFormatException e) {
                log.record(warnEntry().withCause(e).withSource(getClass()).withDescription("Couldn't parse series_number %s", progData.getSeriesNumber().trim()));
            }
        }
    
        series.setPublisher(Publisher.PA);
        series.setSpecialization(specialization(progData, channel));
        setCertificate(progData, series);
        setGenres(progData, series);
        
        selectImages(progData.getPictures(), series, PA_PICTURE_TYPE_SERIES, PA_PICTURE_TYPE_BRAND, Maybe.<String>nothing());
        series.setLastUpdated(updatedAt.toDateTimeUTC());

        return Optional.of(series);
    }
    
    private DateTime fromPaDate(String paDate) {
        if (Strings.isNullOrEmpty(paDate)) {
            return null;
        }
        return PA_DATE_FORMAT.parseDateTime(paDate).withZone(DateTimeZone.UTC);
    }
    
    private ItemAndBroadcast getFilmOrEpisode(ProgData progData, Channel channel, DateTimeZone zone, boolean isEpisode, Timestamp updatedAt) {
        return specialization(progData, channel) == Specialization.FILM ? getFilm(progData, channel, zone, updatedAt) : getEpisode(progData, channel, zone, isEpisode, updatedAt);
    }
    
    private ItemAndBroadcast getFilm(ProgData progData, Channel channel, DateTimeZone zone, Timestamp updatedAt) {
        String filmUri = PaHelper.getFilmUri(identifierFor(progData));
        Maybe<Identified> possiblePreviousData = contentResolver.findByCanonicalUris(ImmutableList.of(filmUri)).getFirstValue();
        
        Film film;
        if (possiblePreviousData.hasValue()) {
        	Identified previous = possiblePreviousData.requireValue();
            if (previous instanceof Film) {
                film = (Film) previous;
            }
            else {
                film = new Film();
                Item.copyTo((Episode) previous, film);
            }
        } else {
            film = getBasicFilm(progData);
        }
        film.addAlias(PaHelper.getFilmAlias(identifierFor(progData)));
        Optional<String> rtFilmIdentifier = rtFilmIdentifierFor(progData);
        if (rtFilmIdentifier.isPresent()) {
            film.addAlias(PaHelper.getRtFilmAlias(rtFilmIdentifier.get()));
        }
        
        film.setAliasUrls(ImmutableSet.of(PaHelper.getAlias(progData.getProgId())));
        
        Broadcast broadcast = setCommonDetails(progData, channel, zone, film, updatedAt);
        
        if (progData.getFilmYear() != null && MoreStrings.containsOnlyAsciiDigits(progData.getFilmYear())) {
            film.setYear(Integer.parseInt(progData.getFilmYear()));
        }
        
        return new ItemAndBroadcast(film, Maybe.just(broadcast));
    }
    
    private Broadcast setCommonDetails(ProgData progData, Channel channel, DateTimeZone zone, Item episode, Timestamp updatedAt) {
                
        //currently Welsh channels have Welsh titles/descriptions 
        // which flip the English ones, resulting in many writes. We'll only take the Welsh title if we don't
    	// already have a title from another channel
        if (episode.getTitle() == null || !channel.getUri().contains("wales")) {
            if (progData.getEpisodeTitle() != null) {
                episode.setTitle(progData.getEpisodeTitle());
            } else {
                episode.setTitle(progData.getTitle());
            }
        }

        if (progData.getBillings() != null) {
            for (Billing billing : progData.getBillings().getBilling()) {
                if((episode.getDescription() == null || !channel.getUri().contains("wales")) 
                        && billing.getType().equals("synopsis")) {
                    episode.setDescription(billing.getvalue());
                }
                if ((episode.getShortDescription() == null || !channel.getUri().contains("wales"))
                        && billing.getType().equals("pa_detail1")) {
                    episode.setShortDescription(billing.getvalue());
                }
                if ((episode.getMediumDescription() == null || !channel.getUri().contains("wales"))
                        && billing.getType().equals("pa_detail2")) {
                    episode.setMediumDescription(billing.getvalue());
                }
                if ((episode.getLongDescription() == null || !channel.getUri().contains("wales"))
                        && billing.getType().equals("pa_detail3")) {
                    episode.setLongDescription(billing.getvalue());
                }
            }
        }

        episode.setMediaType(channel.getMediaType());
        episode.setSpecialization(specialization(progData, channel));
        setGenres(progData, episode);
        
        if (progData.getCountry() != null) {
            episode.setCountriesOfOrigin(countryMap.parseCountries(progData.getCountry()));
        }
        
        if (progData.getAttr() != null) {
            episode.setBlackAndWhite(getBooleanValue(progData.getAttr().getBw()));
        }

        selectImages(progData.getPictures(), episode, PA_PICTURE_TYPE_EPISODE, PA_PICTURE_TYPE_SERIES, Maybe.just(PA_PICTURE_TYPE_BRAND));
        
        episode.setPeople(people(progData));
        
        Version version = findBestVersion(episode.getVersions());
        version.set3d(getBooleanValue(progData.getAttr().getThreeD()));
        Duration duration = Duration.standardMinutes(Long.valueOf(progData.getDuration()));
        version.setDuration(duration);
        setCertificate(progData, episode);

        Broadcast broadcast = broadcast(progData, channel, zone, updatedAt);
        addBroadcast(version, broadcast);
        return broadcast;
    }

    public static Function<Category, String> TO_GENRE_URIS = new Function<Category, String>() {
        @Override
        public String apply(Category from) {
            return "http://pressassociation.com/genres/" + from.getCategoryCode();
        }
    };

    private void setCertificate(ProgData progData, Content content) {
        if (progData.getCertificate() != null) {
            content.setCertificates(ImmutableList.of(new Certificate(progData.getCertificate(), Countries.GB)));
        }
    }
    
    private void setGenres(ProgData progData, Content content) {
        Set<String> extractedGenres = genreMap.map(ImmutableSet.copyOf(Iterables.transform(progData.getCategory(), TO_GENRE_URIS)));
        extractedGenres.remove("http://pressassociation.com/genres/BE00");
        if(!extractedGenres.isEmpty()) {
            content.setGenres(extractedGenres);
        }
    }

    private ItemAndBroadcast getEpisode(ProgData progData, Channel channel, DateTimeZone zone, boolean isEpisode, Timestamp updatedAt) {
        
        String episodeUri = PaHelper.getEpisodeUri(identifierFor(progData));
        Maybe<Identified> possiblePrevious = contentResolver.findByCanonicalUris(ImmutableList.of(episodeUri)).getFirstValue();

        Item item;
        if (possiblePrevious.hasValue()) {
            item = (Item) possiblePrevious.requireValue();
            if (!(item instanceof Episode) && isEpisode) {
                log.record(warnEntry().withSource(getClass()).withDescription("%s resolved as %s being ingested as Episode", episodeUri, item.getClass().getSimpleName()));
                item = convertItemToEpisode(item);
            } else if(item instanceof Episode && !isEpisode) {
                log.record(errorEntry().withSource(getClass()).withDescription("%s resolved as %s being ingested as Item", episodeUri, item.getClass().getSimpleName()));
            }
        } else {
            item = getBasicEpisode(progData, isEpisode);
        }
        
        item.addAlias(PaHelper.getEpisodeAlias(identifierFor(progData)));
        
        Broadcast broadcast = setCommonDetails(progData, channel, zone, item, updatedAt);
        
        try {
            if (item instanceof Episode) {
                Episode episode = (Episode) item;
                episode.setSpecial(getBooleanValue(progData.getAttr().getSpecial()));
                episode.setEpisodeNumber(episodeNumber(progData));
                episode.setSeriesNumber(seriesNumber(progData));
            }
        } catch (NumberFormatException e) {
            // sometimes we don't get valid numbers
            //log.
        }
        
        return new ItemAndBroadcast(item, Maybe.just(broadcast));
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
        Episode episode = new Episode(item.getCanonicalUri(), item.getCurie(),item.getPublisher());
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

    private Broadcast broadcast(ProgData progData, Channel channel, DateTimeZone zone, Timestamp updateAt) {
        Duration duration = Duration.standardMinutes(Long.valueOf(progData.getDuration()));

        DateTime transmissionTime = getTransmissionTime(progData.getDate(), progData.getTime(), zone);
        
        Broadcast broadcast = new Broadcast(channel.getUri(), transmissionTime, duration).withId(PaHelper.getBroadcastId(progData.getShowingId()));
        
        if (progData.getAttr() != null) {
            broadcast.setRepeat(isRepeat(channel, progData.getAttr()));
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
            broadcast.setNewEpisode(isNewEpisode(newSeries, newEpisode));
        }
        broadcast.setLastUpdated(updateAt.toDateTimeUTC());
        return broadcast;
    }
 
    private Boolean isNewEpisode(Boolean newSeries, Boolean newEpisode) {
        return Boolean.TRUE.equals(newSeries) || Boolean.TRUE.equals(newEpisode);
    }

    //If the repeat flag is "yes" it's definitely a repeat. If it's "no" 
    // then we can't be sure so ingest it as null.  
    private Boolean isRepeat(Channel channel, Attr attr) {
        Boolean repeat = getBooleanValue(attr.getRepeat());
        if (Boolean.FALSE.equals(repeat)) {
            return null;
        }
        return repeat;
    }

    private void addBroadcast(Version version, Broadcast newBroadcast) {
        if (Strings.isNullOrEmpty(newBroadcast.getSourceId())) {
            return;
        }
        
        Set<Broadcast> broadcasts = Sets.newHashSet();
        Interval newBroadcastInterval = newBroadcast.transmissionInterval().requireValue();
        
        for (Broadcast existingBroadcast: version.getBroadcasts()) {
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
        
        for (CastMember cast: progData.getCastMember()) {
            if (!Strings.isNullOrEmpty(cast.getActor().getPersonId())) {
                Actor actor = Actor.actor(cast.getActor().getPersonId(), cast.getActor().getvalue(), cast.getCharacter(), Publisher.PA);
                if (! people.contains(actor)) {
                    people.add(actor);
                }
            }
        }
        
        for (StaffMember staffMember: progData.getStaffMember()) {
            if (!Strings.isNullOrEmpty(staffMember.getPerson().getPersonId())) {
                String roleKey = staffMember.getRole().toLowerCase().replace(' ', '_');
                CrewMember crewMember = CrewMember.crewMember(staffMember.getPerson().getPersonId(), staffMember.getPerson().getvalue(), roleKey, Publisher.PA);
                if (! people.contains(crewMember)) {
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
    
    private Film getBasicFilm(ProgData progData) {
        Film film = new Film(PaHelper.getFilmUri(identifierFor(progData)), PaHelper.getFilmCurie(identifierFor(progData)), Publisher.PA);
        
        setBasicDetails(progData, film);
        
        return film;
    }

    private Item getBasicEpisode(ProgData progData, boolean isEpisode) {
        Item item = isEpisode ? new Episode() : new Item();
        item.setCanonicalUri(PaHelper.getEpisodeUri(identifierFor(progData)));
        item.setCurie("pa:e-" + identifierFor(progData));
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

        Duration duration = Duration.standardMinutes(Long.valueOf(progData.getDuration()));
        version.setDuration(duration);

        item.addVersion(version);
    }
    
    protected Specialization specialization(ProgData progData, Channel channel) {
        if (MediaType.AUDIO.equals(channel.getMediaType())) {
            return Specialization.RADIO;
        }
        return Strings.isNullOrEmpty(progData.getRtFilmnumber()) ? Specialization.TV : Specialization.FILM;
    }

    protected static DateTime getTransmissionTime(String date, String time, DateTimeZone zone) {
        String dateString = date + "-" + time;
        return DateTimeFormat.forPattern("dd/MM/yyyy-HH:mm").withZone(zone).parseDateTime(dateString);
    }
    
    protected static String identifierFor(ProgData progData) {
        return ! Strings.isNullOrEmpty(progData.getRtFilmnumber()) ? progData.getRtFilmnumber() : progData.getProgId();
    }
    
    protected Optional<String> rtFilmIdentifierFor(ProgData progData) {
        return Optional.fromNullable(Strings.emptyToNull(progData.getRtFilmnumber()));
    }
    
    private static Boolean getBooleanValue(String value) {
        if (value != null) {
            return value.equalsIgnoreCase(YES);
        }
        return null;
    }
}

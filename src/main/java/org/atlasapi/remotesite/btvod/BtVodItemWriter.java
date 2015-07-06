package org.atlasapi.remotesite.btvod;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collection;
import java.util.Currency;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.metabroadcast.common.currency.Price;
import com.metabroadcast.common.intl.Countries;
import com.metabroadcast.common.intl.Country;

import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Certificate;
import org.atlasapi.media.entity.Clip;
import org.atlasapi.media.entity.Encoding;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Film;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Location;
import org.atlasapi.media.entity.ParentRef;
import org.atlasapi.media.entity.Policy;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Quality;
import org.atlasapi.media.entity.Restriction;
import org.atlasapi.media.entity.Song;
import org.atlasapi.media.entity.Specialization;
import org.atlasapi.media.entity.TopicRef;
import org.atlasapi.media.entity.Version;
import org.atlasapi.media.entity.Policy.RevenueContract;
import org.atlasapi.media.entity.simple.Pricing;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.remotesite.ContentMerger;
import org.atlasapi.remotesite.ContentMerger.MergeStrategy;
import org.atlasapi.remotesite.btvod.model.BtVodEntry;
import org.atlasapi.remotesite.btvod.model.BtVodProductPricingTier;
import org.atlasapi.remotesite.btvod.model.BtVodProductRating;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Ints;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.scheduling.UpdateProgress;


public class BtVodItemWriter implements BtVodDataProcessor<UpdateProgress> {

    private static final String FILM_TYPE = "film";
    private static final String MUSIC_TYPE = "music";
    private static final String EPISODE_TYPE = "episode";
    private static final String COLLECTION_TYPE = "collection";
    private static final String HELP_TYPE = "help";
    private static final String HD_FLAG = "HD";
    private static final String SD_FLAG = "SD";
    private static final List<Pattern> EPISODE_TITLE_PATTERNS = ImmutableList.of(
            Pattern.compile("^.* S[0-9]+\\-E[0-9]+\\s(.*)"),
            Pattern.compile("^.*Season\\s[0-9]+\\s-\\sSeason\\s[0-9]+\\s(Episode\\s[0-9]+.*)"),
            Pattern.compile("^.*?\\-\\s(.*)")
    );
    private static final Logger log = LoggerFactory.getLogger(BtVodItemWriter.class);
    private static final String BT_VOD_GUID_NAMESPACE = "bt:vod:guid";
    private static final String BT_VOD_ID_NAMESPACE = "bt:vod:id";
    private static final Object OTG_PLATFORM = "OTG";
    private static final Object FALSE = "false";


    private final ContentWriter writer;
    private final ContentResolver resolver;
    private final BtVodBrandWriter brandExtractor;
    private final BtVodSeriesWriter seriesExtractor;
    private final Publisher publisher;
    private final String uriPrefix;
    private final ContentMerger contentMerger;
    private final BtVodContentListener listener;
    private final Set<String> processedRows;
    private final Map<String, Item> processedItems;
    private final BtVodDescribedFieldsExtractor describedFieldsExtractor;
    private final TitleSanitiser titleSanitiser;
    private final ImageExtractor imageExtractor;
    private UpdateProgress progress = UpdateProgress.START;
    private BtVodPricingAvailabilityGrouper grouper;

    public BtVodItemWriter(
            ContentWriter writer,
            ContentResolver resolver,
            BtVodBrandWriter brandExtractor,
            BtVodSeriesWriter seriesExtractor,
            Publisher publisher,
            String uriPrefix,
            BtVodContentListener listener,
            BtVodDescribedFieldsExtractor describedFieldsExtractor,
            Set<String> processedRows,
            BtVodPricingAvailabilityGrouper grouper,
            TitleSanitiser titleSanitiser,
            ImageExtractor imageExtractor
    ) {
        this.describedFieldsExtractor = checkNotNull(describedFieldsExtractor);
        this.grouper = checkNotNull(grouper);
        this.listener = checkNotNull(listener);
        this.writer = checkNotNull(writer);
        this.resolver = checkNotNull(resolver);
        this.brandExtractor = checkNotNull(brandExtractor);
        this.seriesExtractor = checkNotNull(seriesExtractor);
        this.publisher = checkNotNull(publisher);
        this.uriPrefix = checkNotNull(uriPrefix);
        this.contentMerger = new ContentMerger(MergeStrategy.REPLACE, MergeStrategy.REPLACE, MergeStrategy.REPLACE);
        this.processedRows = checkNotNull(processedRows);
        this.titleSanitiser = checkNotNull(titleSanitiser);
        this.processedItems = Maps.newHashMap();
        this.imageExtractor = checkNotNull(imageExtractor);
    }

    @Override
    public boolean process(BtVodEntry row) {
        UpdateProgress thisProgress = UpdateProgress.FAILURE;
        try {
            if (!shouldProcess(row)
                    || processedRows.contains(row.getGuid())) {
                thisProgress = UpdateProgress.SUCCESS;
                return true;
            }

            Item item = itemFrom(row);
            processedItems.put(itemKeyForDeduping(row), item);
            write(item);
            processedRows.add(row.getGuid());
            listener.onContent(item, row);
            thisProgress = UpdateProgress.SUCCESS;
        } catch (Exception e) {
            log.error("Failed to process row: " + row.toString(), e);
        } finally {
            progress = progress.reduce(thisProgress);
        }
        return true;
    }

    private void write(Item extracted) {
        Maybe<Identified> existing = resolver
                .findByCanonicalUris(ImmutableSet.of(extracted.getCanonicalUri()))
                .getFirstValue();

        if (existing.hasValue()) {
            Item merged = contentMerger.merge((Item) existing.requireValue(),
                    extracted);
            writer.createOrUpdate(merged);
        } else {
            writer.createOrUpdate(extracted);
        }
    }

    private boolean shouldProcess(BtVodEntry row) {
        return !COLLECTION_TYPE.equals(row.getProductType()) && !HELP_TYPE.equals(row.getProductType());
    }

    private boolean isEpisode(BtVodEntry row) {
        return EPISODE_TYPE.equals(row.getProductType()) && getBrandRefOrNull(row) != null;
    }

    private Item itemFrom(BtVodEntry row) {
        Item item;
        String itemKeyForDeduping = itemKeyForDeduping(row);
        if (processedItems.containsKey(itemKeyForDeduping)) {
            item = processedItems.get(itemKeyForDeduping);
            includeVersionsAndClipsOnAlreadyExtractedItem(item, row);
            return item;
        }
        if (isEpisode(row)) {
            item = createEpisode(row);
        } else if (FILM_TYPE.equals(row.getProductType())) {
            item = createFilm(row);
        } else if (MUSIC_TYPE.equals(row.getProductType())) {
            item = createSong(row);
        } else {
            item = createItem(row);
        }
        populateItemFields(item, row);
        return item;
    }

    private void includeVersionsAndClipsOnAlreadyExtractedItem(Item item, BtVodEntry row) {
        item.addVersions(createVersions(row));
        item.addClips(extractTrailer(row));
        item.addAliases(describedFieldsExtractor.aliasesFrom(row));
    }

    private String itemKeyForDeduping(BtVodEntry row) {
        return row.getProductType() + ":" + titleSanitiser.sanitiseTitle(row.getTitle());
    }

    private Item createSong(BtVodEntry row) {
        Song song = new Song(uriFor(row), null, publisher);
        song.setTitle(titleForNonEpisode(row));
        song.setSpecialization(Specialization.MUSIC);
        return song;
    }

    private Episode createEpisode(BtVodEntry row) {
        Episode episode = new Episode(uriFor(row), null, publisher);
        episode.setSeriesNumber(extractSeriesNumber(row));
        episode.setEpisodeNumber(extractEpisodeNumber(row));
        episode.setTitle(extractEpisodeTitle(row.getTitle()));
        episode.setSeriesRef(getSeriesRefOrNull(row));
        episode.setParentRef(getBrandRefOrNull(row));
        episode.setSpecialization(Specialization.TV);
        return episode;
    }
    
    public Integer extractSeriesNumber(BtVodEntry row) {
        return seriesExtractor.extractSeriesNumber(row.getTitle()).orNull();
    }


    public static Integer extractEpisodeNumber(BtVodEntry row) {
        String episodeNumber = Iterables.getOnlyElement(
                row.getProductScopes()
        ).getProductMetadata().getEpisodeNumber();
        if (episodeNumber == null) {
            return null;
        }
        return Ints.tryParse(episodeNumber);
    }

    private ParentRef getSeriesRefOrNull(BtVodEntry row) {

        Optional<ParentRef> seriesRef = seriesExtractor.getSeriesRefFor(row);
        if (!seriesRef.isPresent()) {
            log.warn("Episode without series {}", row.getTitle());
        }
        return seriesRef.orNull();
    }

    private ParentRef getBrandRefOrNull(BtVodEntry row) {
        Optional<ParentRef> brandRef = brandExtractor.getBrandRefFor(row);
        if (!brandRef.isPresent()) {
            log.warn("Episode without brand {}", row.getTitle());
        }
        return brandRef.orNull();
    }

    /**
     * An episode title has usually the form of "Scrubs S4-E18 My Roommates"
     * In this case we want to extract the real episode title "My Roommates"
     * We also remove string " - HD" suffix
     * Otherwise we leave the title untouched
     */
    public String extractEpisodeTitle(String title) {
        if (title == null) {
            return null;
        }

        for (Pattern titlePattern : EPISODE_TITLE_PATTERNS) {
            Matcher matcher = titlePattern.matcher(title);
            if (matcher.matches()) {
                return titleSanitiser.sanitiseTitle(matcher.group(1));
            }
        }
        return titleSanitiser.sanitiseTitle(title);
    }

    private Item createItem(BtVodEntry row) {
        Item item = new Item(uriFor(row), null, publisher);
        item.setSpecialization(Specialization.TV);
        return item;
    }

    private Film createFilm(BtVodEntry row) {
        Film film = new Film(uriFor(row), null, publisher);
        film.setYear(Ints.tryParse(Iterables.getOnlyElement(row.getProductScopes()).getProductMetadata().getReleaseYear()));
        film.setTitle(titleForNonEpisode(row));
        film.setSpecialization(Specialization.FILM);
        return film;
    }

    private void populateItemFields(Item item, BtVodEntry row) {
        Optional<ParentRef> brandRefFor = brandExtractor.getBrandRefFor(row);


        if (brandRefFor.isPresent()) {
            item.setParentRef(brandRefFor.get());
        }

        describedFieldsExtractor.setDescribedFieldsFrom(row, item);

        item.setVersions(createVersions(row));
        item.setEditorialPriority(row.getProductPriority());
        item.addTopicRefs(describedFieldsExtractor.topicFor(row).asSet());
        item.addTopicRefs(describedFieldsExtractor.btGenresFrom(row));
        item.setImages(imageExtractor.extractImages(row));

        BtVodProductRating rating = Iterables.getFirst(row.getplproduct$ratings(), null);
        if (rating != null) {
            item.setCertificates(ImmutableList.of(new Certificate(rating.getProductRating(), Countries.GB)));
        }

        item.setClips(extractTrailer(row));
    }

    private List<Clip> extractTrailer(BtVodEntry row) {
        if (!isTrailerMediaAvailableOnCdn(row)
                || Strings.isNullOrEmpty(row.getProductTrailerMediaId())) {
            return ImmutableList.of();
        }

        Clip clip = new Clip(row.getProductTrailerMediaId(),
                             row.getProductTrailerMediaId(),
                             publisher);

        Version version = new Version();
        Encoding encoding = new Encoding();
        setQualityOn(encoding, row);
        version.addManifestedAs(encoding);
        clip.addVersion(version);
        return ImmutableList.of(clip);
    }

    private boolean isTrailerMediaAvailableOnCdn(BtVodEntry row) {
        return true;
        //return row.getTrailerServiceTypes().contains(OTG_PLATFORM);
    }

    private String titleForNonEpisode(BtVodEntry row) {
        return titleSanitiser.sanitiseTitle(row.getTitle());
    }

    private String uriFor(BtVodEntry row, RevenueContract revenueContract) {
        String id = row.getGuid();
        return uriPrefix + "items/" + id + "/" + revenueContract.toString();
    }

    private String uriFor(BtVodEntry row) {
        String id = row.getGuid();
        return uriPrefix + "items/" + id;
    }

    @Override
    public UpdateProgress getResult() {
        return progress;
    }

    private Set<Version> createVersions(BtVodEntry row) {

        Set<Alias> aliases = ImmutableSet.of(
                                new Alias(BT_VOD_GUID_NAMESPACE, row.getGuid()),
                                new Alias(BT_VOD_ID_NAMESPACE, row.getId())
                             );

        if (row.getProductOfferStartDate() == null
                || row.getProductOfferEndDate() == null
                || !isItemTvodPlayoutAllowed(row)
                || !isItemMediaAvailableOnCdn(row)) {
            return ImmutableSet.of();
        }

        ImmutableSet.Builder<Location> locations = ImmutableSet.builder();
        if (!row.getSubscriptionCodes().isEmpty()) {
            DateTime availabilityStart = new DateTime(row.getProductOfferStartDate(), DateTimeZone.UTC);
            DateTime availabilityEnd = new DateTime(row.getProductOfferEndDate(), DateTimeZone.UTC);
            locations.add(createLocation(row, new Interval(availabilityStart, availabilityEnd),
                    ImmutableSet.<BtVodProductPricingTier>of(), RevenueContract.SUBSCRIPTION, aliases));
        }

        //TODO filter for blackout
        if (!row.getProductPricingPlan().getProductPricingTiers().isEmpty()) {
            if (row.getProductOfferingType().contains("-EST")) {
                locations.addAll(createLocations(row, RevenueContract.PAY_TO_BUY, aliases));
            } else {
                locations.addAll(createLocations(row, RevenueContract.PAY_TO_RENT, aliases));
            }
        }

        Encoding encoding = new Encoding();
        encoding.setAvailableAt(locations.build());
        setQualityOn(encoding, row);

        Version version = new Version();
        version.setManifestedAs(ImmutableSet.of(encoding));
        version.setCanonicalUri(uriFor(row));
        version.setAliases(aliases);

        if (row.getProductDuration() != null) {
            version.setDuration(Duration.standardSeconds(row.getProductDuration()));
        }

        BtVodProductRating rating = Iterables.getFirst(row.getplproduct$ratings(), null);
        if (rating != null) {
            String scheme = rating.getProductScheme();
            String ratingValue = rating.getProductRating();

//            version.setRestriction(Restriction.from(scheme, ratingValue));
        }

        return ImmutableSet.of(version);
    }

    private Set<Location> createLocations(BtVodEntry row, RevenueContract subscription, Set<Alias> aliases) {
        ImmutableSet.Builder<Location> locations = ImmutableSet.builder();
        Multimap<Interval, BtVodProductPricingTier> groupAvailabilityPeriods = grouper.groupAvailabilityPeriods(row);
        for (Entry<Interval, Collection<BtVodProductPricingTier>> entry : groupAvailabilityPeriods.asMap().entrySet()) {
            locations.add(createLocation(row, entry.getKey(), entry.getValue(), subscription, aliases));
        }
        return locations.build();
    }

    private Location createLocation(BtVodEntry row, Interval availability, Collection<BtVodProductPricingTier> pricingTiers,
            RevenueContract subscription, Set<Alias> aliases) {

        Policy policy = new Policy();
        policy.setAvailabilityStart(availability.getStart());
        policy.setAvailabilityEnd(availability.getEnd());
        ImmutableList.Builder<Pricing> pricings = ImmutableList.builder();
        for (BtVodProductPricingTier pricingTier : pricingTiers) {
            DateTime startDate = new DateTime(pricingTier.getProductAbsoluteStart(), DateTimeZone.UTC);
            DateTime endDate = new DateTime(pricingTier.getProductAbsoluteEnd(), DateTimeZone.UTC);
            Double amount;
            if (pricingTier.getProductAmounts().getGBP() == null) {
                amount = 0D;
            } else {
                amount = pricingTier.getProductAmounts().getGBP();
            }
            Price price = new Price(Currency.getInstance("GBP"), amount);
            pricings.add(new Pricing(startDate, endDate, price));
        }
        policy.setPricing(pricings.build());
        policy.setSubscriptionPackages(row.getSubscriptionCodes());
        policy.setAvailableCountries(ImmutableSet.of(Countries.GB));

        if (!row.getSubscriptionCodes().isEmpty()) {
            policy.setRevenueContract(RevenueContract.SUBSCRIPTION);
        } else {
            if (row.getProductOfferingType().contains("-EST")) {
                policy.setRevenueContract(RevenueContract.PAY_TO_BUY);
            } else {
                policy.setRevenueContract(RevenueContract.PAY_TO_RENT);
            }
        }
        Location location = new Location();
        location.setPolicy(policy);
        location.setCanonicalUri(uriFor(row, policy.getRevenueContract()));
        location.setUri(uriFor(row));
        location.setAliases(aliases);

        return location;

    }

    private boolean isItemMediaAvailableOnCdn(BtVodEntry row) {
        return true;
        //return row.getServiceTypes().contains(OTG_PLATFORM);
    }

    private boolean isItemTvodPlayoutAllowed(BtVodEntry row) {
        return true;
        //return !FALSE.equals(row.getMasterAgreementOtgTvodPlay());
    }

    private void setQualityOn(Encoding encoding, BtVodEntry entry) {
        if (HD_FLAG.equals(entry.getProductTargetBandwidth())) {
            encoding.setHighDefinition(true);
            encoding.setQuality(Quality.HD);
        } else if (SD_FLAG.equals(entry.getProductTargetBandwidth())) {
            encoding.setHighDefinition(false);
            encoding.setQuality(Quality.SD);
        }
    }
}

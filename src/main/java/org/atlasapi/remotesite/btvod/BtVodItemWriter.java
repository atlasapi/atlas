package org.atlasapi.remotesite.btvod;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Currency;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.metabroadcast.common.currency.Price;
import com.metabroadcast.common.intl.Countries;
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
import org.atlasapi.media.entity.Restriction;
import org.atlasapi.media.entity.Song;
import org.atlasapi.media.entity.Version;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Ints;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.scheduling.UpdateProgress;


public class BtVodItemWriter implements BtVodDataProcessor<UpdateProgress> {

    private static final String BEFORE_DVD_SUFFIX = " (Before DVD)";
    private static final String FILM_TYPE = "film";
    private static final String MUSIC_TYPE = "music";
    private static final String EPISODE_TYPE = "episode";
    private static final String COLLECTION_TYPE = "collection";
    private static final String HELP_TYPE = "help";
    private static final Pattern HD_PATTERN = Pattern.compile("^(.*)\\-\\sHD");
    private static final String HD_FLAG = "HD";
    private static final String SD_FLAG = "SD";
    private static final List<Pattern> EPISODE_TITLE_PATTERNS = ImmutableList.of(
            Pattern.compile("^.* S[0-9]+\\-E[0-9]+\\s(.*)"),
            Pattern.compile("^.*Season\\s[0-9]+\\s-\\sSeason\\s[0-9]+\\s(Episode\\s[0-9]+.*)"),
            Pattern.compile("^.*?\\-\\s(.*)")
    );
    private static final Logger log = LoggerFactory.getLogger(BtVodItemWriter.class);
    private static final String COMING_SOON_SUFFIX = ": Coming Soon";
    private static final String BT_VOD_GUID_NAMESPACE = "bt:vod:guid";
    private static final String BT_VOD_ID_NAMESPACE = "bt:vod:id";


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
    private UpdateProgress progress = UpdateProgress.START;

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
            TitleSanitiser titleSanitiser
    ) {
        this.describedFieldsExtractor = describedFieldsExtractor;
        this.listener = checkNotNull(listener);
        this.writer = checkNotNull(writer);
        this.resolver = checkNotNull(resolver);
        this.brandExtractor = checkNotNull(brandExtractor);
        this.seriesExtractor = checkNotNull(seriesExtractor);
        this.publisher = checkNotNull(publisher);
        this.uriPrefix = checkNotNull(uriPrefix);
        this.contentMerger = new ContentMerger(MergeStrategy.REPLACE, MergeStrategy.KEEP, MergeStrategy.REPLACE);
        this.processedRows = checkNotNull(processedRows);
        this.titleSanitiser = checkNotNull(titleSanitiser);
        this.processedItems = Maps.newHashMap();
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
            processedItems.put(stripHDSuffix(row.getTitle()), item);
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
        if (processedItems.containsKey(stripHDSuffix(row.getTitle()))) {
            item = processedItems.get(stripHDSuffix(row.getTitle()));
            item.addVersions(createVersions(row));
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
        sanitiseTitle(item);
        return item;
    }

    private void sanitiseTitle(Item item) {
        String title = item.getTitle();
        if (title == null) {
            return;
        }

        item.setTitle(title.replace(BEFORE_DVD_SUFFIX, "").replace(COMING_SOON_SUFFIX, ""));
    }

    private Item createSong(BtVodEntry row) {
        Song song = new Song(uriFor(row), null, publisher);
        song.setTitle(titleForNonEpisode(row));
        return song;
    }

    private Episode createEpisode(BtVodEntry row) {
        Episode episode = new Episode(uriFor(row), null, publisher);
        episode.setSeriesNumber(seriesExtractor.extractSeriesNumber(row.getTitle()).orNull());
        episode.setEpisodeNumber(extractEpisodeNumber(row));
        episode.setTitle(extractEpisodeTitle(row.getTitle()));
        episode.setSeriesRef(getSeriesRefOrNull(row));
        episode.setParentRef(getBrandRefOrNull(row));

        return episode;
    }


    private Integer extractEpisodeNumber(BtVodEntry row) {
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
                return titleSanitiser.sanitiseTitle(stripHDSuffix(matcher.group(1)));

            }

        }
        return titleSanitiser.sanitiseTitle(title);
    }

    private String stripHDSuffix(String title) {
        Matcher hdMatcher = HD_PATTERN.matcher(title);
        if (hdMatcher.matches()) {
            return hdMatcher.group(1).trim().replace("- HD ", "");
        }
        return title;
    }

    private Item createItem(BtVodEntry row) {
        Item item = new Item(uriFor(row), null, publisher);
        item.setTitle(titleForNonEpisode(row));
        return item;
    }

    private Film createFilm(BtVodEntry row) {
        Film film = new Film(uriFor(row), null, publisher);
        film.setYear(Ints.tryParse(Iterables.getOnlyElement(row.getProductScopes()).getProductMetadata().getReleaseYear()));
        film.setTitle(titleForNonEpisode(row));
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

        BtVodProductRating rating = Iterables.getFirst(row.getplproduct$ratings(), null);
        if (rating != null) {
            item.setCertificates(ImmutableList.of(new Certificate(rating.getPlproduct$ratingString(), Countries.GB)));
        }
        if (row.getProductTrailerMediaId() != null) {
            item.setClips(
                    ImmutableSet.of(
                            new Clip(row.getProductTrailerMediaId(), row.getProductTrailerMediaId(), publisher)
                    )
            );
        }
    }

    private String titleForNonEpisode(BtVodEntry row) {
        return stripHDSuffix(row.getTitle());
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
        if (row.getProductOfferStartDate() == null
                || row.getProductOfferEndDate() == null) {
            return ImmutableSet.of();
        }

        DateTime availabilityStart = new DateTime(row.getProductOfferStartDate(), DateTimeZone.UTC);
        DateTime availabilityEnd = new DateTime(row.getProductOfferEndDate(), DateTimeZone.UTC);

        Policy policy = new Policy();
        policy.setAvailabilityStart(availabilityStart);
        policy.setAvailabilityEnd(availabilityEnd);
        ImmutableList.Builder<Pricing> pricings = ImmutableList.builder();
        for (BtVodProductPricingTier pricingTier : row.getProductPricingPlan().getProductPricingTiers()) {
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


        Location location = new Location();
        location.setPolicy(policy);
        location.setCanonicalUri(uriFor(row));
        location.setUri(uriFor(row));

        Alias btVodGuidAlias = new Alias(BT_VOD_GUID_NAMESPACE, row.getGuid());
        Alias btVodIdAlias = new Alias(BT_VOD_ID_NAMESPACE, row.getId());
        location.setAliases(ImmutableSet.of(btVodGuidAlias, btVodIdAlias));
        location.setAliasUrls(ImmutableSet.of(uriFor(row), row.getId()));


        Encoding encoding = new Encoding();
        encoding.setAvailableAt(ImmutableSet.of(location));
        if (HD_FLAG.equals(row.getProductTargetBandwidth())) {
            encoding.setHighDefinition(true);
        } else if (SD_FLAG.equals(row.getProductTargetBandwidth())) {
            encoding.setHighDefinition(false);
        }

        Version version = new Version();
        version.setManifestedAs(ImmutableSet.of(encoding));
        version.setCanonicalUri(uriFor(row));
        version.setAliasUrls(location.getAliasUrls());
        version.setAliases(location.getAliases());

        BtVodProductRating rating = Iterables.getFirst(row.getplproduct$ratings(), null);
        if (rating != null) {
            Integer ageRating = rating.getProductRating();
            if (ageRating != null) {
                version.setRestriction(Restriction.from(ageRating, rating.getProductScheme()));
            } else {
                version.setRestriction(
                        Restriction.from(
                                String.format(
                                        "%s:%s",
                                        rating.getProductScheme(),
                                        rating.getPlproduct$ratingString()
                                )
                        )
                );
            }
        }

        return ImmutableSet.of(version);
    }
}

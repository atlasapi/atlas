package org.atlasapi.remotesite.btvod;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Currency;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.currency.Price;
import org.atlasapi.media.entity.Encoding;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Film;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Location;
import org.atlasapi.media.entity.ParentRef;
import org.atlasapi.media.entity.Policy;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Song;
import org.atlasapi.media.entity.Version;
import org.atlasapi.media.entity.simple.Pricing;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.remotesite.ContentMerger;
import org.atlasapi.remotesite.ContentMerger.MergeStrategy;
import org.atlasapi.remotesite.btvod.model.BtVodEntry;
import org.atlasapi.remotesite.btvod.model.BtVodPlproduct$pricingTier;
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
    private static final Pattern EPISODE_TITLE_PATTERN = Pattern.compile("^.* S[0-9]+\\-E[0-9]+ (.*)");
    private static final Pattern EPISODE_NUMBER_PATTERN = Pattern.compile("S[0-9]+\\-E([0-9]+)");
    private static final Logger log = LoggerFactory.getLogger(BtVodItemWriter.class);
    private static final String COMING_SOON_SUFFIX = ": Coming Soon";

    private final ContentWriter writer;
    private final ContentResolver resolver;
    private final BtVodBrandWriter brandExtractor;
    private final BtVodSeriesWriter seriesExtractor;
    private final Publisher publisher;
    private final String uriPrefix;
    private final ContentMerger contentMerger;
    private final BtVodContentListener listener;
    private final Set<String> processedRows;
    private final BtVodDescribedFieldsExtractor describedFieldsExtractor;
    private UpdateProgress progress = UpdateProgress.START;

    public BtVodItemWriter(ContentWriter writer, ContentResolver resolver,
            BtVodBrandWriter brandExtractor, BtVodSeriesWriter seriesExtractor,
            Publisher publisher, String uriPrefix, BtVodContentListener listener,
            BtVodDescribedFieldsExtractor describedFieldsExtractor,
            Set<String> processedRows) {
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
        return !COLLECTION_TYPE.equals(row.getBtproduct$productType()) && !HELP_TYPE.equals(row.getBtproduct$productType());
    }

    private boolean isValidHierarchy(BtVodEntry row) {
        return EPISODE_TYPE.equals(row.getBtproduct$productType()) && seriesExtractor.getSeriesRefFor(row).isPresent()
                        && brandExtractor.getBrandRefFor(row).isPresent();
    }
    
    private Item itemFrom(BtVodEntry row) {
        Item item;
        if (isValidHierarchy(row)) {
            item = createEpisode(row);
        } else if (FILM_TYPE.equals(row.getBtproduct$productType())) {
            item = createFilm(row);
        } else if (MUSIC_TYPE.equals(row.getBtproduct$productType())) {
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
        episode.setSeriesNumber(seriesExtractor.extractSeriesNumber(row.getTitle()));
        episode.setEpisodeNumber(extractEpisodeNumber(row.getTitle()));
        episode.setTitle(extractEpisodeTitle(row.getTitle()));
        episode.setSeriesRef(getSeriesRefOrNull(row));
        
        return episode;
    }

    private Integer extractEpisodeNumber(String title) {
        if (title == null) {
            return null;
        }

        Matcher matcher = EPISODE_NUMBER_PATTERN.matcher(title);

        if (matcher.find()) {
            return Ints.tryParse(matcher.group(1));
        }

        return null;
    }

    private ParentRef getSeriesRefOrNull(BtVodEntry row) {
        return seriesExtractor.getSeriesRefFor(row)
                .orNull();
    }

    /**
     * An episode title has usually the form of "Scrubs S4-E18 My Roommates"
     * In this case we want to extract the real episode title "My Roommates"
     * We also remove string " - HD" suffix
     * Otherwise we leave the title untouched
     */
    private String extractEpisodeTitle(String title) {
        if (title == null) {
            return null;
        }

        Matcher matcher = EPISODE_TITLE_PATTERN.matcher(title);

        if (matcher.matches()) {
            return stripHDSuffix(matcher.group(1));

        }

        return title;
    }

    private String stripHDSuffix(String title) {
        Matcher hdMatcher = HD_PATTERN.matcher(title);
        if (hdMatcher.matches()) {
            return hdMatcher.group(1);
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
        film.setYear(Ints.tryParse(Iterables.getOnlyElement(row.getPlproduct$scopes()).getPlproduct$productMetadata().getReleaseYear()));
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
        item.setEditorialPriority(row.getBtproduct$priority());
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
        if (row.getPlproduct$offerStartDate() == null
                || row.getPlproduct$offerEndDate() == null) {
            return ImmutableSet.of();
        }
        
        DateTime availabilityStart = new DateTime(row.getPlproduct$offerStartDate(), DateTimeZone.UTC);
        DateTime availabilityEnd = new DateTime(row.getPlproduct$offerEndDate(), DateTimeZone.UTC);
        
        Policy policy = new Policy();
        policy.setAvailabilityStart(availabilityStart);
        policy.setAvailabilityEnd(availabilityEnd);
        ImmutableList.Builder<Pricing> pricings = ImmutableList.builder();
        for (BtVodPlproduct$pricingTier pricingTier : row.getPlproduct$pricingPlan().getPlproduct$pricingTiers()) {
            DateTime startDate = new DateTime(pricingTier.getPlproduct$absoluteStart(), DateTimeZone.UTC);
            DateTime endDate = new DateTime(pricingTier.getPlproduct$absoluteEnd(), DateTimeZone.UTC);
            Double amount;
            if (pricingTier.getPlproduct$amounts().getGBP() == null) {
                amount = 0D;
            } else {
                amount = pricingTier.getPlproduct$amounts().getGBP();
            }
            Price price = new Price(Currency.getInstance("GBP"), amount);
            pricings.add(new Pricing(startDate, endDate, price));
        }
        policy.setPricing(pricings.build());


        Location location = new Location();
        location.setPolicy(policy);
        
        Encoding encoding = new Encoding();
        encoding.setAvailableAt(ImmutableSet.of(location));
        
        Version version = new Version();
        version.setManifestedAs(ImmutableSet.of(encoding));
        
        return ImmutableSet.of(version);
    }
}

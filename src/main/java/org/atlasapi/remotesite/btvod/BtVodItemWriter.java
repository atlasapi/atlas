package org.atlasapi.remotesite.btvod;

import static com.google.common.base.Preconditions.checkNotNull;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Set;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.remotesite.ContentMerger;
import org.atlasapi.remotesite.ContentMerger.MergeStrategy;
import org.atlasapi.remotesite.btvod.BtVodData.BtVodDataRow;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Ints;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.scheduling.UpdateProgress;


public class BtVodItemWriter implements BtVodDataProcessor<UpdateProgress> {

    private static final String BEFORE_DVD_SUFFIX = " (Before DVD)";
    private static final String FILM_CATEGORY = "Film";
    private static final String MUSIC_CATEGORY = "Music";
    private static final SimpleDateFormat FORMATTER = new SimpleDateFormat("MMM dd yyyy hh:mmaa");
    private static final Pattern EPISODE_TITLE_PATTERN = Pattern.compile("^.* S[0-9]+\\-E[0-9]+ (.*)");
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
        
        FORMATTER.setTimeZone(TimeZone.getTimeZone("UTC"));
    }
    
    @Override
    public boolean process(BtVodDataRow row) {
        UpdateProgress thisProgress = UpdateProgress.FAILURE;
        try {
            if (!shouldProcess(row) 
                    || processedRows.contains(row.getColumnValue(BtVodFileColumn.PRODUCT_ID))) {
                thisProgress = UpdateProgress.SUCCESS;
                return true;
            }
            
            Item item = itemFrom(row);
            write(item);
            processedRows.add(row.getColumnValue(BtVodFileColumn.PRODUCT_ID));
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

    private boolean shouldProcess(BtVodDataRow row) {
        String serviceFormat = row.getColumnValue(BtVodFileColumn.SERVICE_FORMAT);
        return !"Y".equals(row.getColumnValue(BtVodFileColumn.IS_SERIES))
                && serviceFormat != null && serviceFormat.contains("Youview")
                && isValidHierarchy(row);
    }

    // Ignore top-level series, these aren't really, but they're duff data
    private boolean isValidHierarchy(BtVodDataRow row) {
        return Strings.isNullOrEmpty(row.getColumnValue(BtVodFileColumn.SERIES_NUMBER))
                || (!Strings.isNullOrEmpty(row.getColumnValue(BtVodFileColumn.SERIES_NUMBER))
                        && brandExtractor.getBrandRefFor(row).isPresent());
    }
    
    private Item itemFrom(BtVodDataRow row) {
        Item item;
        if (brandExtractor.getBrandRefFor(row).isPresent()
                && !Strings.isNullOrEmpty(row.getColumnValue(BtVodFileColumn.SERIES_NUMBER))) {
            item = createEpisode(row);
        } else if (FILM_CATEGORY.equals(row.getColumnValue(BtVodFileColumn.CATEGORY))) {
            item = createFilm(row);
        } else if (MUSIC_CATEGORY.equals(row.getColumnValue(BtVodFileColumn.CATEGORY))){
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

    private Item createSong(BtVodDataRow row) {
        Song song = new Song(uriFor(row), null, publisher);
        song.setTitle(titleForNonEpisode(row));
        return song;
    }

    private Episode createEpisode(BtVodDataRow row) {
        Episode episode = new Episode(uriFor(row), null, publisher);
        episode.setSeriesNumber(Ints.tryParse(row.getColumnValue(BtVodFileColumn.SERIES_NUMBER)));
        episode.setEpisodeNumber(Ints.tryParse(row.getColumnValue(BtVodFileColumn.EPISODE_NUMBER)));
        String fullTitle = Strings.emptyToNull(row.getColumnValue(BtVodFileColumn.EPISODE_TITLE));
        episode.setTitle(extractEpisodeTitle(fullTitle));
        episode.setSeriesRef(getSeriesRefOrNull(row));
        
        return episode;
    }

    private ParentRef getSeriesRefOrNull(BtVodDataRow row) {
        return seriesExtractor.getSeriesRefFor(row.getColumnValue(BtVodFileColumn.SERIES_TITLE))
                .orNull();
    }

    /**
     * An episode title has usually the form of "Scrubs S4-E18 My Roommates"
     * In this case we want to extract the real episode title "My Roommates"
     * Otherwise we leave the title untouched
     */
    private String extractEpisodeTitle(String title) {
        if (title == null) {
            return null;
        }

        Matcher matcher = EPISODE_TITLE_PATTERN.matcher(title);

        if (matcher.matches()) {
            return matcher.group(1);
        }

        return title;
    }

    private Item createItem(BtVodDataRow row) {
        Item item = new Item(uriFor(row), null, publisher);
        item.setTitle(titleForNonEpisode(row));
        return item;
    }
    
    private Film createFilm(BtVodDataRow row) {
        Film film = new Film(uriFor(row), null, publisher);
        film.setYear(Ints.tryParse(row.getColumnValue(BtVodFileColumn.RELEASE_YEAR)));
        film.setTitle(titleForNonEpisode(row));
        return film;
    }
    
    private void populateItemFields(Item item, BtVodDataRow row) {
        Optional<ParentRef> brandRefFor = brandExtractor.getBrandRefFor(row);

        if (brandRefFor.isPresent()) {
            item.setParentRef(brandRefFor.get());
        }

        describedFieldsExtractor.setDescribedFieldsFrom(row, item);
        item.setVersions(createVersions(row));
    }
    
    private String titleForNonEpisode(BtVodDataRow row) {
        String assetTitle = Strings.emptyToNull(row.getColumnValue(BtVodFileColumn.ASSET_TITLE));
        if (assetTitle != null) {
            return assetTitle;
        }
        
        return Strings.emptyToNull(row.getColumnValue(BtVodFileColumn.PRODUCT_TITLE));
    }

    private String uriFor(BtVodDataRow row) {
        String id = row.getColumnValue(BtVodFileColumn.PRODUCT_ID);
        return uriPrefix + "items/" + id;
    }
    
    @Override
    public UpdateProgress getResult() {
        return progress;
    }
    
    private Set<Version> createVersions(BtVodDataRow row) {
        if (Strings.isNullOrEmpty(row.getColumnValue(BtVodFileColumn.AVAILABILITY_START))
                || Strings.isNullOrEmpty(row.getColumnValue(BtVodFileColumn.AVAILABILITY_END))) {
            return ImmutableSet.of();
        }
        
        DateTime availabilityStart = dateTimeFrom(row, BtVodFileColumn.AVAILABILITY_START);
        DateTime availabilityEnd = dateTimeFrom(row, BtVodFileColumn.AVAILABILITY_END);
        
        Policy policy = new Policy();
        policy.setAvailabilityStart(availabilityStart);
        policy.setAvailabilityEnd(availabilityEnd);
        
        Location location = new Location();
        location.setPolicy(policy);
        
        Encoding encoding = new Encoding();
        encoding.setAvailableAt(ImmutableSet.of(location));
        
        Version version = new Version();
        version.setManifestedAs(ImmutableSet.of(encoding));
        
        return ImmutableSet.of(version);
    }

    private DateTime dateTimeFrom(BtVodDataRow row, BtVodFileColumn column) {
        try {
            Date parsed = FORMATTER.parse(row.getColumnValue(column));
            return new DateTime(parsed).withZone(DateTimeZone.UTC);
        } catch (ParseException e) {
            Throwables.propagate(e);
        }
        return null;
    }
}

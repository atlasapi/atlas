package org.atlasapi.remotesite.btvod;

import static com.google.common.base.Preconditions.checkNotNull;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Set;

import joptsimple.internal.Strings;

import org.atlasapi.media.entity.Encoding;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Film;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Image;
import org.atlasapi.media.entity.ImageType;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Location;
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
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Ints;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.scheduling.UpdateProgress;


public class BtVodItemExtractor implements BtVodDataProcessor<UpdateProgress> {

    private static final String FILM_CATEGORY = "Film";
    private static final String MUSIC_CATEGORY = "Music";
    private static final String IMAGE_URI_PREFIX = "http://portal.vision.bt.com/btvo/";
    private static final SimpleDateFormat FORMATTER = new SimpleDateFormat("MMM dd yyyy hh:mmaa");

    private static final Logger log = LoggerFactory.getLogger(BtVodItemExtractor.class);
    
    private final ContentWriter writer;
    private final ContentResolver resolver;
    private final BtVodBrandExtractor brandExtractor;
    private final BtVodSeriesExtractor seriesExtractor;
    private final Publisher publisher;
    private final String uriPrefix;
    private final ContentMerger contentMerger;
    private UpdateProgress progress = UpdateProgress.START;
    private BtVodContentListener listener;

    public BtVodItemExtractor(ContentWriter writer, ContentResolver resolver,
            BtVodBrandExtractor brandExtractor, BtVodSeriesExtractor seriesExtractor,
            Publisher publisher, String uriPrefix, BtVodContentListener listener) {
        this.listener = checkNotNull(listener);
        this.writer = checkNotNull(writer);
        this.resolver = checkNotNull(resolver);
        this.brandExtractor = checkNotNull(brandExtractor);
        this.seriesExtractor = checkNotNull(seriesExtractor);
        this.publisher = checkNotNull(publisher);
        this.uriPrefix = checkNotNull(uriPrefix);
        this.contentMerger = new ContentMerger(MergeStrategy.REPLACE, MergeStrategy.KEEP, MergeStrategy.REPLACE);
    }
    
    @Override
    public boolean process(BtVodDataRow row) {
        UpdateProgress thisProgress = UpdateProgress.FAILURE;
        try {
            if (!shouldProcess(row)) {
                thisProgress = UpdateProgress.SUCCESS;
                return true;
            }
            
            Item item = itemFrom(row);
            write(item);
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
                        && !Strings.isNullOrEmpty(row.getColumnValue(BtVodFileColumn.BRANDIA_ID)));
    }
    
    private Item itemFrom(BtVodDataRow row) {
        Item item;
        if (!Strings.isNullOrEmpty(row.getColumnValue(BtVodFileColumn.BRANDIA_ID))
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
        return item;
    }

    private Item createSong(BtVodDataRow row) {
        return new Song(uriFor(row), null, publisher);
    }

    private Episode createEpisode(BtVodDataRow row) {
        Episode episode = new Episode(uriFor(row), null, publisher);
        episode.setSeriesNumber(Ints.tryParse(row.getColumnValue(BtVodFileColumn.EPISODE_NUMBER)));
        episode.setSeriesRef(
                seriesExtractor.getSeriesRefFor(
                                    row.getColumnValue(BtVodFileColumn.BRANDIA_ID), 
                                    episode.getSeriesNumber())
                         );
        
        return episode;
    }
    
    private Item createItem(BtVodDataRow row) {
        return new Item(uriFor(row), null, publisher);
    }
    
    private Film createFilm(BtVodDataRow row) {
        Film film = new Film(uriFor(row), null, publisher);
        film.setYear(Ints.tryParse(row.getColumnValue(BtVodFileColumn.RELEASE_YEAR)));
        return film;
    }
    
    private void populateItemFields(Item item, BtVodDataRow row) {
        //TODO: Choose the title more carefully from the various
        //options available to us
        item.setTitle(row.getColumnValue(BtVodFileColumn.EPISODE_TITLE));
        String brandId = row.getColumnValue(BtVodFileColumn.BRANDIA_ID);
        if (brandId != null) {
            item.setParentRef(brandExtractor.getBrandRefFor(brandId));
        }
        item.setDescription(row.getColumnValue(BtVodFileColumn.SYNOPSIS));
        item.setVersions(createVersions(row));
        item.setImages(createImages(row));
    }
    
    private Iterable<Image> createImages(BtVodDataRow row) {
        String packshotFilename = row.getColumnValue(BtVodFileColumn.PACKSHOT);
        if (Strings.isNullOrEmpty(packshotFilename)) {
            return ImmutableSet.of();
        }
        
        Image image = new Image(IMAGE_URI_PREFIX + packshotFilename);
        image.setType(ImageType.PRIMARY);
        return ImmutableSet.of(image);
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

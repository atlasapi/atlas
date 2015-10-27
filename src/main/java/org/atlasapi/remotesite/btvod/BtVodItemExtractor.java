package org.atlasapi.remotesite.btvod;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Certificate;
import org.atlasapi.media.entity.Clip;
import org.atlasapi.media.entity.Encoding;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Film;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.ParentRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Series;
import org.atlasapi.media.entity.Song;
import org.atlasapi.media.entity.Specialization;
import org.atlasapi.media.entity.TopicRef;
import org.atlasapi.media.entity.Version;
import org.atlasapi.remotesite.btvod.model.BtVodEntry;
import org.atlasapi.remotesite.btvod.model.BtVodProductRating;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import com.metabroadcast.common.intl.Countries;
import com.metabroadcast.common.scheduling.UpdateProgress;


public class BtVodItemExtractor implements BtVodDataProcessor<UpdateProgress> {
    
    private static final String FILM_TYPE = "film";
    private static final String MUSIC_TYPE = "music";
    static final String EPISODE_TYPE = "episode";
    private static final String COLLECTION_TYPE = "collection";
    private static final String HELP_TYPE = "help";

    private static final Logger log = LoggerFactory.getLogger(BtVodItemExtractor.class);
    private static final String OTG_PLATFORM = "OTG";

    private final BtVodBrandProvider brandProvider;
    private final BtVodSeriesProvider seriesProvider;
    private final Publisher publisher;
    private final String uriPrefix;
    private final BtVodContentListener listener;
    private final Set<String> processedRows;

    private final Map<String, Item> processedItems;

    private final BtVodDescribedFieldsExtractor describedFieldsExtractor;
    private final TitleSanitiser titleSanitiser;
    private final ImageExtractor imageExtractor;
    private UpdateProgress progress = UpdateProgress.START;
    private final BtVodVersionsExtractor versionsExtractor;

    private final ImmutableSet<TopicRef> topicsToPropagateToParents;

    public BtVodItemExtractor(
            BtVodBrandProvider brandProvider,
            BtVodSeriesProvider seriesProvider,
            Publisher publisher,
            String uriPrefix,
            BtVodContentListener listener,
            BtVodDescribedFieldsExtractor describedFieldsExtractor,
            Set<String> processedRows,
            TitleSanitiser titleSanitiser,
            ImageExtractor imageExtractor,
            BtVodVersionsExtractor versionsExtractor,
            TopicRef newTopic,
            TopicRef kidsTopic,
            TopicRef tvTopic,
            TopicRef subscriptionCatchupTopic
    ) {
        this.brandProvider = checkNotNull(brandProvider);
        this.describedFieldsExtractor = checkNotNull(describedFieldsExtractor);
        this.listener = checkNotNull(listener);
        this.seriesProvider = checkNotNull(seriesProvider);
        this.publisher = checkNotNull(publisher);
        this.uriPrefix = checkNotNull(uriPrefix);
        this.processedRows = checkNotNull(processedRows);
        this.titleSanitiser = checkNotNull(titleSanitiser);
        this.processedItems = Maps.newHashMap();
        this.imageExtractor = checkNotNull(imageExtractor);
        this.versionsExtractor = checkNotNull(versionsExtractor);
        this.topicsToPropagateToParents = ImmutableSet.of(newTopic, kidsTopic, tvTopic, subscriptionCatchupTopic);
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
        addTopicsToParents(item, row);
        return item;
    }

    private void addTopicsToParents(Item item, BtVodEntry row) {
        for (TopicRef topicRef : topicsToPropagateToParents) {
            if (item.getTopicRefs().contains(topicRef)) {
                if (item.getContainer() != null) {
                    Brand brand = brandProvider.brandFor(row).get();
                    brand.addTopicRef(topicRef);
                    listener.onContent(brand, row);
                }
                if(item instanceof Episode && ((Episode)item).getSeriesRef() != null) {
                    Series series = seriesProvider.seriesFor(row).get();
                    series.addTopicRef(topicRef);
                    listener.onContent(series, row);
                }
            }
        }
    }

    private void includeVersionsAndClipsOnAlreadyExtractedItem(Item item, BtVodEntry row) {
        item.addVersions(versionsExtractor.createVersions(row));
        item.addClips(extractTrailer(row));
        item.addAliases(describedFieldsExtractor.aliasesFrom(row));
    }

    private String itemKeyForDeduping(BtVodEntry row) {
        return row.getProductType() + ":" + titleSanitiser.sanitiseTitle(row.getTitle());
    }

    private Item createSong(BtVodEntry row) {
        Song song = new Song(uriFor(row), null, publisher);
        song.setTitle(titleSanitiser.sanitiseSongTitle(row.getTitle()));
        song.setSpecialization(Specialization.MUSIC);
        return song;
    }

    private Episode createEpisode(BtVodEntry row) {
        Episode episode = new Episode(uriFor(row), null, publisher);
        episode.setSeriesNumber(extractSeriesNumber(row));
        episode.setEpisodeNumber(extractEpisodeNumber(row));
        episode.setTitle(titleSanitiser.sanitiseTitle(row.getTitle()));
        episode.setSeriesRef(getSeriesRefOrNull(row));
        episode.setParentRef(getBrandRefOrNull(row));
        episode.setSpecialization(Specialization.TV);
        return episode;
    }

    public Integer extractSeriesNumber(BtVodEntry row) {
        Optional<Series> seriesRef = seriesProvider.seriesFor(row);
        if(!seriesRef.isPresent()) {
            return null;
        }
        return seriesRef.get().getSeriesNumber();
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

        Optional<Series> series = seriesProvider.seriesFor(row);
        if (!series.isPresent()) {
            log.warn("Episode without series {}", row.getTitle());
            return null;
        }
        return ParentRef.parentRefFrom(series.get());
    }

    private ParentRef getBrandRefOrNull(BtVodEntry row) {
        Optional<ParentRef> brandRef = brandProvider.brandRefFor(row);
        if (!brandRef.isPresent()) {
            log.warn("Episode without brand {}", row.getTitle());
        }
        return brandRef.orNull();
    }

    private Item createItem(BtVodEntry row) {
        Item item = new Item(uriFor(row), null, publisher);
        item.setTitle(titleSanitiser.sanitiseTitle(row.getTitle()));
        item.setSpecialization(Specialization.TV);
        return item;
    }

    private Film createFilm(BtVodEntry row) {
        Film film = new Film(uriFor(row), null, publisher);
        film.setYear(Ints.tryParse(Iterables.getOnlyElement(row.getProductScopes()).getProductMetadata().getReleaseYear()));
        film.setTitle(titleSanitiser.sanitiseTitle(row.getTitle()));
        film.setSpecialization(Specialization.FILM);
        return film;
    }

    private void populateItemFields(Item item, BtVodEntry row) {
        Optional<ParentRef> brandRefFor = brandProvider.brandRefFor(row);


        if (brandRefFor.isPresent()) {
            item.setParentRef(brandRefFor.get());
        }

        describedFieldsExtractor.setDescribedFieldsFrom(row, item);

        item.setVersions(versionsExtractor.createVersions(row));
        item.setEditorialPriority(row.getProductPriority());

        VodEntryAndContent vodEntryAndContent = new VodEntryAndContent(row, item);
        item.addTopicRefs(describedFieldsExtractor.topicsFrom(vodEntryAndContent));
        item.setImages(imageExtractor.imagesFor(row));

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
        versionsExtractor.setQualityOn(encoding, row);
        version.addManifestedAs(encoding);
        clip.addVersion(version);
        return ImmutableList.of(clip);
    }

    private boolean isTrailerMediaAvailableOnCdn(BtVodEntry row) {
        return row.getTrailerServiceTypes().contains(OTG_PLATFORM);
    }

    private String uriFor(BtVodEntry row) {
        String id = row.getGuid();
        return uriPrefix + "items/" + id;
    }

    @Override
    public UpdateProgress getResult() {
        return progress;
    }

    public Map<String, Item> getProcessedItems() {
        return ImmutableMap.copyOf(processedItems);
    }
}
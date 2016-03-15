package org.atlasapi.remotesite.btvod;

import java.util.List;
import java.util.Map;
import java.util.Set;

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

import com.metabroadcast.common.intl.Countries;
import com.metabroadcast.common.scheduling.UpdateProgress;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.atlasapi.remotesite.btvod.BtVodProductType.COLLECTION;
import static org.atlasapi.remotesite.btvod.BtVodProductType.EPISODE;
import static org.atlasapi.remotesite.btvod.BtVodProductType.FILM;
import static org.atlasapi.remotesite.btvod.BtVodProductType.HELP;
import static org.atlasapi.remotesite.btvod.BtVodProductType.MUSIC;
import static org.atlasapi.remotesite.btvod.BtVodProductType.SEASON;


public class BtVodItemExtractor implements BtVodDataProcessor<UpdateProgress> {
    
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
    private final BtVodEntryMatchingPredicate kidsPredicate;
    private UpdateProgress progress = UpdateProgress.START;
    private final BtVodVersionsExtractor versionsExtractor;
    private final DedupedDescriptionAndImageUpdater descriptionAndImageUpdater;

    private final BtVodEpisodeNumberExtractor episodeNumberExtractor;
    private final BtMpxVodClient mpxClient;

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
            DedupedDescriptionAndImageUpdater descriptionAndImageUpdater,
            BtVodEpisodeNumberExtractor episodeNumberExtractor,
            BtMpxVodClient mpxClient,
            BtVodEntryMatchingPredicate kidsPredicate
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
        this.descriptionAndImageUpdater = checkNotNull(descriptionAndImageUpdater);
        this.episodeNumberExtractor = checkNotNull(episodeNumberExtractor);
        this.mpxClient = checkNotNull(mpxClient);
        this.kidsPredicate = checkNotNull(kidsPredicate);
    }

    @Override
    public boolean process(BtVodEntry row) {
        UpdateProgress thisProgress = UpdateProgress.FAILURE;
        try {
            if (!shouldProcess(row) || processedRows.contains(row.getGuid())) {
                thisProgress = UpdateProgress.SUCCESS;
                return true;
            }

            Item item = itemFrom(row);

            if(item instanceof Episode) {
                seriesProvider.updateSeriesFromEpisode(row, (Episode) item);
                brandProvider.updateBrandFromEpisode(row, (Episode) item);
            }

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
        return !COLLECTION.isOfType(row.getProductType())
                    && !HELP.isOfType(row.getProductType())
                    && !SEASON.isOfType(row.getProductType())
                    && !isKidsEst(row);
    }

    private boolean isKidsEst(BtVodEntry entry) {
        return kidsPredicate.apply(entry)
                && entry.getProductOfferingType() != null
                && entry.getProductOfferingType().contains(BtVodVersionsExtractor.PAY_TO_BUY_SUFFIX);
    }

    private boolean isEpisode(BtVodEntry row) {
        return EPISODE.isOfType(row.getProductType()) && getBrandRefOrNull(row) != null;
    }

    private Item itemFrom(BtVodEntry row) {
        Item item;
        String itemKeyForDeduping = itemKeyForDeduping(row);
        log.debug("GUID [{}] Key for deduping [{}]", row.getGuid(), itemKeyForDeduping);
        if (processedItems.containsKey(itemKeyForDeduping)) {
            item = processedItems.get(itemKeyForDeduping);
            log.debug("Already found matching item with same key, its URI is {}", item.getCanonicalUri());
            includeMetadataOnExistingItem(item, row);
            return item;
        }
        if (isEpisode(row)) {
            log.debug("GUID [{}] Creating episode", row.getGuid());
            item = createEpisode(row);
        } else if (FILM.isOfType(row.getProductType())) {
            log.debug("GUID [{}] Creating film", row.getGuid());
            item = createFilm(row);
        } else if (MUSIC.isOfType(row.getProductType())) {
            log.debug("GUID [{}] Creating song", row.getGuid());
            item = createSong(row);
        } else {
            log.debug("GUID [{}] Creating item", row.getGuid());
            item = createItem(row);
        }
        log.debug("GUID [{}] Created content with URI {}", row.getGuid(), item.getCanonicalUri());
        populateItemFields(item, row);
        return item;
    }

    private void includeMetadataOnExistingItem(Item item, BtVodEntry row) {
        Set<Version> versions = versionsExtractor.createVersions(row);
        descriptionAndImageUpdater.updateDescriptionsAndImages(
                item, row, imageExtractor.imagesFor(row), versions
        );
        item.addVersions(versions);

        item.addClips(extractTrailer(row));
        item.addAliases(describedFieldsExtractor.explicitAliasesFrom(row));
        
        addMissingTopicRefs(item, row);
    }
    
    private void addMissingTopicRefs(final Item item, BtVodEntry row) {
        final Set<Long> existingTopicIds = topicIdsOfReferencedTopics(item);
        
        VodEntryAndContent vodEntryAndContent = new VodEntryAndContent(row, item);
        Iterable<TopicRef> additionalTopicRefs = Iterables.filter(describedFieldsExtractor.topicsFrom(vodEntryAndContent),
                new Predicate<TopicRef>() {

                    @Override
                    public boolean apply(final TopicRef newTopic) {
                        return !existingTopicIds.contains(newTopic.getTopic());
                    }
                }
        );
        
        item.addTopicRefs(additionalTopicRefs);
    }

    private Set<Long> topicIdsOfReferencedTopics(final Item item) {
        return Sets.newHashSet(
                Iterables.transform(item.getTopicRefs(), 
                                    new Function<TopicRef, Long>() {

                                        @Override
                                        public Long apply(TopicRef input) {
                                            return input.getTopic();
                                        }
                                    })
                              );
    }
    
    private String itemKeyForDeduping(BtVodEntry row) {
        ParentRef brandRef = getBrandRefOrNull(row);
        String brandUri = brandRef != null ? brandRef.getUri() : "";
        String title = titleSanitiser.sanitiseTitle(row.getTitle());
        
        ParentRef seriesRef = getSeriesRefOrNull(row);
        String seriesUri = seriesRef != null ? seriesRef.getUri() : "";
        
        // An empty title may happen, in which case we don't want to 
        // merge all items with empty titles into the same item. 
        // To avoid that, we use the GUID as the item's key, so it'll
        // not be found as a merge candidate by another item
        if (Strings.isNullOrEmpty(title)) {
            title = row.getGuid();
        }
        
        return row.getProductType() + ":" + brandUri + seriesUri + title;
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
        if (Strings.isNullOrEmpty(episode.getTitle()) && episode.getEpisodeNumber() != null) {
            episode.setTitle(String.format("Episode %d", episode.getEpisodeNumber()));
        }
        episode.setSeriesRef(getSeriesRefOrNull(row));
        episode.setParentRef(getBrandRefOrNull(row));
        episode.setSpecialization(Specialization.TV);
        return episode;
    }

    public Integer extractSeriesNumber(BtVodEntry row) {
        if (!Strings.isNullOrEmpty(row.getParentGuid())) {
            Optional<BtVodEntry> parent = mpxClient.getItem(row.getParentGuid());
            if (parent.isPresent() && COLLECTION.isOfType(parent.get().getProductType())) {
                return null;
            }
        }
        Optional<Series> seriesRef = seriesProvider.seriesFor(row);
        if(!seriesRef.isPresent()) {
            return null;
        }
        return seriesRef.get().getSeriesNumber();
    }


    public Integer extractEpisodeNumber(BtVodEntry row) {
        return episodeNumberExtractor.extractEpisodeNumber(row);
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
        item.setAliases(describedFieldsExtractor.explicitAliasesFrom(row));

        Set<Version> versions = versionsExtractor.createVersions(row);
        descriptionAndImageUpdater.updateDescriptionsAndImages(
                item, row, imageExtractor.imagesFor(row), versions
        );
        item.setVersions(versions);

        item.setEditorialPriority(row.getProductPriority());

        VodEntryAndContent vodEntryAndContent = new VodEntryAndContent(row, item);
        item.addTopicRefs(describedFieldsExtractor.topicsFrom(vodEntryAndContent));

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

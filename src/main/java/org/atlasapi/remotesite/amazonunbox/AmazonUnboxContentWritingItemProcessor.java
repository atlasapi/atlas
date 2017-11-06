package org.atlasapi.remotesite.amazonunbox;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Encoding;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Film;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Location;
import org.atlasapi.media.entity.ParentRef;
import org.atlasapi.media.entity.Policy;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Series;
import org.atlasapi.media.entity.Version;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.listing.ContentLister;
import org.atlasapi.persistence.content.listing.ContentListingCriteria;
import org.atlasapi.remotesite.ContentExtractor;
import org.atlasapi.remotesite.ContentMerger;
import org.atlasapi.remotesite.ContentMerger.MergeStrategy;
import org.atlasapi.remotesite.bbc.nitro.ModelWithPayload;
import org.atlasapi.reporting.telescope.OwlTelescopeReporter;

import com.metabroadcast.columbus.telescope.client.EntityType;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.stream.MoreCollectors;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import javafx.application.Platform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.atlasapi.remotesite.amazonunbox.AmazonUnboxContentExtractor.URI_PREFIX;

public class AmazonUnboxContentWritingItemProcessor implements AmazonUnboxItemProcessor {

    private static final Ordering<Content> REVERSE_HIERARCHICAL_ORDER = new Ordering<Content>() {
        @Override
        public int compare(Content left, Content right) {
            if (left instanceof Item) {
                if (right instanceof Item) {
                    return 0;
                } else {
                    return -1;
                }
            } else if (left instanceof Series) {
                if (right instanceof Item) {
                    return 1;
                } else if (right instanceof Series) {
                    return 0;
                } else {
                    return -1;
                }
            } else {
                if (right instanceof Brand) {
                    return 0;
                } else {
                    return 1;
                }
            }
        }
    };

    public static final String CONTAINER = "CONTAINER";
    public static final String EPISODE = "EPISODE";
    public static final String ITEM = "ITEM";
    public static final String BRAND = "BRAND";
    public static final String SERIES = "SERIES";
    public static final String FILM = "FILM";
    public static final String GB_AMAZON_ASIN = "gb:amazon:asin";
    public static final String UNPUBLISH_NO_PAYLOAD_STRING = "Unpublished item has no payload";

    private final Predicate<Alias> AMAZON_ALIAS =
            input -> GB_AMAZON_ASIN.equals(input.getNamespace());
    private final Predicate<String> AMAZON_ALIAS_URL =
            input -> input.startsWith(URI_PREFIX);


    private final Logger log = LoggerFactory.getLogger(
            AmazonUnboxContentWritingItemProcessor.class
    );
    private final Map<String, ModelWithPayload<? extends Container>>
            seenContainer = Maps.newHashMap();
    private final SetMultimap<String, ModelWithPayload<? extends Content>>
            cached = HashMultimap.create();
    private final Map<String, Brand> topLevelSeries = Maps.newHashMap();
    private final Map<String, Brand> standAloneEpisodes = Maps.newHashMap();
    private final BiMap<String, ModelWithPayload<Content>> seenContent = HashBiMap.create();
    private final Map<String, String> duplicatedSeriesToCommonKeyMap = Maps.newHashMap();
    private final Map<String, ModelWithPayload<Series>>
            commonSeriesKeyToCommonSeriesMap = Maps.newHashMap();
    private final Map<String, String> duplicatedBrandToCommonKeyMap = Maps.newHashMap();
    private final Map<String, ModelWithPayload<Brand>>
            commonBrandKeyToCommonBrandMap = Maps.newHashMap();

    private final ContentExtractor<AmazonUnboxItem, Iterable<Content>> extractor;
    private final ContentResolver resolver;
    private final ContentWriter writer;
    private final ContentLister lister;
    private final int missingContentPercentage;
    private final AmazonUnboxBrandProcessor brandProcessor;
    private final ContentMerger contentMerger;

    public AmazonUnboxContentWritingItemProcessor(
            ContentExtractor<AmazonUnboxItem, Iterable<Content>> extractor,
            ContentResolver resolver,
            ContentWriter writer,
            ContentLister lister,
            int missingContentPercentage,
            AmazonUnboxBrandProcessor brandProcessor
    ) {
        this.extractor = checkNotNull(extractor);
        this.resolver = checkNotNull(resolver);
        this.writer = checkNotNull(writer);
        this.lister = checkNotNull(lister);
        this.missingContentPercentage = checkNotNull(missingContentPercentage);
        this.brandProcessor = checkNotNull(brandProcessor);
        this.contentMerger = new ContentMerger(
                MergeStrategy.REPLACE,
                MergeStrategy.REPLACE,
                MergeStrategy.REPLACE
        );
    }

    @Override
    public void prepare() {
        seenContainer.clear();
        cached.clear();
        topLevelSeries.clear();
        standAloneEpisodes.clear();
        seenContent.clear();
    }
    
    @Override
    public void process(AmazonUnboxItem item) {
        for (Content content : extract(item)) {

            ModelWithPayload<Content> contentWithPayload = new ModelWithPayload(content, item);
            checkForDuplicatesSaveForProcessing(contentWithPayload);
        }
    }

    private void checkForDuplicatesSaveForProcessing(ModelWithPayload<Content> content) {
        StringBuilder key = new StringBuilder();
        if (content.getModel() instanceof Container) {

            if (content.getModel() instanceof Brand) {
                ModelWithPayload<Brand> brand = content.asModelType(Brand.class);
                String title = brand.getModel().getTitle();
                key.append(title).append(BRAND);
                duplicatedBrandToCommonKeyMap.put(
                        brand.getModel().getCanonicalUri(),
                        key.toString());
                if (!commonBrandKeyToCommonBrandMap.containsKey(key.toString())) {
                    commonBrandKeyToCommonBrandMap.put(key.toString(), brand);
                }
            } else if (content.getModel() instanceof Series){
                ModelWithPayload<Series> series = content.asModelType(Series.class);
                String title = series.getModel().getTitle();
                String brandTitle = series.getModel().getShortDescription();
                key.append(title).append(brandTitle).append(SERIES);
                duplicatedSeriesToCommonKeyMap.put(
                        series.getModel().getCanonicalUri(),
                        key.toString());
                if (!commonSeriesKeyToCommonSeriesMap.containsKey(key.toString())) {
                    commonSeriesKeyToCommonSeriesMap.put(key.toString(), series);
                }
            }
            String hash = key.toString();
            if (!seenContent.containsKey(hash) && !seenContent.containsValue(content)) {
                seenContent.put(hash, content);
            }

        } else {

            if (content.getModel() instanceof Episode){
                Episode episode = (Episode) content.getModel();
                String title = episode.getTitle();
                int episodeNumber = episode.getEpisodeNumber();
                int seasonNumber = episode.getSeriesNumber();
                String brandTitle = episode.getShortDescription();
                key
                        .append(title)
                        .append(brandTitle)
                        .append(episodeNumber)
                        .append(seasonNumber)
                        .append(EPISODE);
            } else if (content.getModel() instanceof Film) {
                Film film = (Film) content.getModel();
                String title = film.getTitle();
                Integer year = film.getYear();
                if (film.getYear() != null) {
                    key.append(year);
                }
                key.append(title).append(FILM);
            } else {
                Item item = (Item) content.getModel();
                String title = item.getTitle();
                key.append(title).append(ITEM);
            }
            String hash = key.toString();
            if (!seenContent.containsKey(hash)) {
                seenContent.put(hash, content);
            } else {
                ModelWithPayload<Content> seen = seenContent.get(hash);
                seenContent.forcePut(hash, mergeAliasesAndLocations(content, seen));
            }
        }
    }

    private ModelWithPayload<Content> mergeAliasesAndLocations(
            ModelWithPayload<Content> item,
            ModelWithPayload<Content> seen) {
        Version version = Iterables.getOnlyElement(item.getModel().getVersions());
        Alias alias = Iterables.getOnlyElement(Iterables.filter(item.getModel().getAliases(),
                AMAZON_ALIAS::test
        ));

        String aliasUrl = item.getModel()
                .getAliasUrls()
                .stream()
                .filter(AMAZON_ALIAS_URL)
                .findFirst().get(); //we want it to throw an exception if its empty

        version.addAlias(alias);
        version.addAliasUrl(aliasUrl);
        seen.getModel().addVersion(version);
        seen.getModel().addAlias(alias);
        seen.getModel().addAliasUrl(aliasUrl);
        return seen;
    }

    @Override
    public void finish(OwlTelescopeReporter telescope) {
        processSeenContent(telescope);
        processTopLevelSeries(telescope);
        processStandAloneEpisodes(telescope);
        
        if (cached.values().size() > 0) {
            log.warn("{} extracted but unwritten", cached.values().size());
            for (Entry<String, Collection<ModelWithPayload<? extends Content>>> mapping
                    : cached.asMap().entrySet()) {

                for (ModelWithPayload<? extends Content> contentWithPayload : mapping.getValue()) {
                    telescope.reportFailedEvent(
                            contentWithPayload.getModel().getId(),
                            "Content has been extracted but not written",
                            contentWithPayload.getModel(),
                            contentWithPayload.getPayload()
                            );
                    log.warn("Unwritten: {} - {} ", mapping.getKey(), contentWithPayload );
                }
            }
        }

        seenContainer.clear();
        cached.clear();
        topLevelSeries.clear();
        standAloneEpisodes.clear();
        
        checkForDeletedContent(telescope);
        
        seenContent.clear();
    }

    private void processSeenContent(OwlTelescopeReporter telescope) {
        //Process Brands first, Series next and everything else last.
        List<ModelWithPayload<Content>> brands = new ArrayList<>();
        List<ModelWithPayload<Content>> series = new ArrayList<>();
        List<ModelWithPayload<Content>> notContainers = new ArrayList<>();

        for (ModelWithPayload<Content> container : seenContent.values()){
            if (container.getModel() instanceof Brand){
                brands.add(container);
            } else if (container.getModel() instanceof Series){
                series.add(container);
            } else {
                notContainers.add(container);
            }
        }

        brands.forEach(c -> checkAndWriteSeenContent(c, telescope));
        series.forEach(c -> checkAndWriteSeenContent(c, telescope));
        notContainers.forEach(c -> checkAndWriteSeenContent(c, telescope));
    }

    private void checkAndWriteSeenContent(
            ModelWithPayload<Content> content,
            OwlTelescopeReporter telescope) {
        Maybe<Identified> existing = resolve(content.getModel().getCanonicalUri());
        if (existing.isNothing()) {
            write(content, telescope);
        } else {
            Identified identified = existing.requireValue();
            if (content.getModel() instanceof Item) {
                write(mergeItemsWithPayload(
                        ContentMerger.asItem(identified),
                        content.asModelType(Item.class)),
                        telescope);
            } else if (content.getModel() instanceof Container) {
                write(mergeContainersWithPayload(
                        ContentMerger.asContainer(identified),
                        content.asModelType(Container.class)),
                        telescope);
            }
        }
    }

    private ModelWithPayload<? extends Item> mergeItemsWithPayload(
            Item existingContent,
            ModelWithPayload<? extends Item> contentWithPayload
    ) {
            Item merged = contentMerger.merge(existingContent, contentWithPayload.getModel());
            return new ModelWithPayload<>(merged, contentWithPayload.getPayload());
    }


    private ModelWithPayload<? extends Container> mergeContainersWithPayload(
            Container existingContent,
            ModelWithPayload<? extends Container> contentWithPayload
    ) {
        Container merged = contentMerger.merge(existingContent, contentWithPayload.getModel());
        return new ModelWithPayload<>(merged, contentWithPayload.getPayload());
    }

    private void processStandAloneEpisodes(OwlTelescopeReporter telescope) {
        for (Entry<String, Brand> entry : standAloneEpisodes.entrySet()) {

            List<ModelWithPayload<Item>> cachedItems =
                    cached.get(entry.getKey()).stream()
                            .filter(c -> c.getModel() instanceof Item)
                            .map(c -> c.asModelType(Item.class))
                            .collect(Collectors.toList());

            ModelWithPayload<Item> item = Iterables.getOnlyElement(cachedItems);

            cached.removeAll(entry.getKey());
            
            if (item.getModel() instanceof Episode) {
                Episode episode = (Episode) item.getModel();
                episode.setSeriesRef(null);
                episode.setSeriesNumber(null);
            }
            item.getModel().setParentRef(null);
            write(item, telescope);
        }
    }

    private void processTopLevelSeries(OwlTelescopeReporter telescope) {
        for (Entry<String, Brand> entry : topLevelSeries.entrySet()) {

            List<ModelWithPayload<Series>> cachedSeries =
                    cached.get(entry.getKey()).stream()
                            .filter(c -> c.getModel() instanceof Series)
                            .map(c -> c.asModelType(Series.class))
                            .collect(Collectors.toList());

            ModelWithPayload<Series> series = Iterables.getOnlyElement(cachedSeries);

            cached.remove(entry.getKey(), series);
            
            series.getModel().setParentRef(null);
            String seriesUri = series.getModel().getCanonicalUri();
            // TODO will this update the refs?
            for (ModelWithPayload<? extends Content> child : cached.get(seriesUri)) {
                Episode episode = (Episode) child.getModel();
                episode.setParentRef(ParentRef.parentRefFrom(series.getModel()));
                episode.setSeriesRef(null);
                episode.setSeriesNumber(null);
                //write(episode);
            }
            // this will write those items cached against the series
            write(series, telescope);
            
            for (ModelWithPayload<? extends Content> child : cached.removeAll(entry.getKey())) {
                if (child.getModel() instanceof Item) {
                    ModelWithPayload<Item> item = child.asModelType(Item.class);
                    if (item.getModel() instanceof Episode) {
                        Episode episode = (Episode) child.getModel();
                        episode.setSeriesRef(null);
                        episode.setSeriesNumber(null);
                    } 
                    item.getModel().setParentRef(ParentRef.parentRefFrom(series.getModel()));
                    write(item, telescope);
                }
            }
        }
    }
     
    private void checkForDeletedContent(OwlTelescopeReporter telescope) {
        Set<Content> allAmazonContent = ImmutableSet.copyOf(resolveAllAmazonUnboxContent());
        Set<Content> contents = seenContent.values().stream()
                .map(ModelWithPayload::getModel)
                .collect(MoreCollectors.toImmutableSet());
        Set<Content> notSeen = Sets.difference(
                allAmazonContent,
                contents
        );
        
        float missingPercentage =
                ((float) notSeen.size() / (float) allAmazonContent.size()) * 100;
        if (missingPercentage > (float) missingContentPercentage) {
            telescope.reportFailedEvent("File failed to update " +
                    missingPercentage +
                    "% of all Amazon content. File may be truncated.");
            throw new RuntimeException("File failed to update " +
                            missingPercentage +
                            "% of all Amazon content. File may be truncated.");
        } else {
            List<Content> orderedContent = REVERSE_HIERARCHICAL_ORDER.sortedCopy(notSeen);
            for (Content notSeenContent : orderedContent) {
                notSeenContent.setActivelyPublished(false);
                if (notSeenContent instanceof Item) {
                    writer.createOrUpdate((Item) notSeenContent);

                    reportContentToTelescope(
                            notSeenContent,
                            EntityType.ITEM,
                            "Atlas did not return an id after attempting to " +
                                    "create or update this Item",
                            telescope
                    );
                } else if (notSeenContent instanceof Container) {
                    writer.createOrUpdate((Container) notSeenContent);

                    reportContentToTelescope(
                            notSeenContent,
                            EntityType.CONTAINER,
                            "Atlas did not return an id after attempting to " +
                                    "create or update this Container",
                            telescope
                    );

                } else {
                    telescope.reportFailedEvent(
                            "This item could not be written to Atlas. id=" + notSeenContent +
                                    " (Amazon content with uri" +
                                    notSeenContent.getCanonicalUri() +
                                    "not an Item or a Container",
                            UNPUBLISH_NO_PAYLOAD_STRING
                    );

                    throw new RuntimeException("Amazon Unbox content with uri " +
                            notSeenContent.getCanonicalUri() + " not an Item or a Container");
                }
            }
        }
    }

    private Iterator<Content> resolveAllAmazonUnboxContent() {
        ContentListingCriteria criteria = ContentListingCriteria
            .defaultCriteria()
            .forPublisher(Publisher.AMAZON_UNBOX)
            .build();
        
        return lister.listContent(criteria);
    }
    
    public Iterable<Content> extract(AmazonUnboxItem item) {
        return extractor.extract(item);
    }
    
    private Maybe<Identified> resolve(String uri) {
        ImmutableSet<String> uris = ImmutableSet.of(uri);
        return resolver.findByCanonicalUris(uris).get(uri);
    }
    
    private void write(ModelWithPayload<? extends Content> content, OwlTelescopeReporter telescope) {
        if (content.getModel() instanceof Container) {
            if (content.getModel() instanceof Series) {
                cacheOrWriteSeriesAndSubContents(content.asModelType(Series.class), telescope);
            } else if (content.getModel() instanceof Brand) {
                cacheOrWriteBrandAndCachedSubContents(content.asModelType(Brand.class), telescope);
            } else {
                writer.createOrUpdate(content.asModelType(Container.class).getModel());

                reportContentWithPayloadToTelescope(
                        content,
                        EntityType.CONTAINER,
                        "Atlas did not return an id after attempting to " +
                                "create or update this Container",
                        telescope
                );

            }
        } else if (content.getModel() instanceof Item) {
            if (content.getModel() instanceof Episode) {
                cacheOrWriteEpisode(content.asModelType(Episode.class), telescope);
            } else {
                cacheOrWriteItem(content.asModelType(Content.class), telescope);
            }
        }
    }

    private void cacheOrWriteBrandAndCachedSubContents(
            ModelWithPayload<Brand> brand,
            OwlTelescopeReporter telescope) {
        String brandUri = brand.getModel().getCanonicalUri();
        BrandType brandType = brandProcessor.getBrandType(brandUri);
        if (BrandType.TOP_LEVEL_SERIES.equals(brandType)) {
            log.trace("Caching top-level series " + brandUri);
            topLevelSeries.put(brandUri, brand.getModel());
        } else if (BrandType.STAND_ALONE_EPISODE.equals(brandType)) {
            log.trace("Caching stand-alone episode " + brandUri);
            standAloneEpisodes.put(brandUri, brand.getModel());
        } else {
            writeBrand(brand, telescope);
        }
    }
    
    private void writeBrand(ModelWithPayload<Brand> brand, OwlTelescopeReporter telescope) {
        writer.createOrUpdate(brand.getModel());

        reportContentWithPayloadToTelescope(
                brand,
                EntityType.BRAND,
                "Atlas did not return an id after attempting to create or update this Brand",
                telescope);

        String brandUri = brand.getModel().getCanonicalUri();
        seenContainer.put(brandUri, brand);
        for (ModelWithPayload<? extends Content> subContent : cached.removeAll(brandUri)) {
            write(subContent, telescope);
        }
    }

    private void cacheOrWriteSeriesAndSubContents(
            ModelWithPayload<Series> series,
            OwlTelescopeReporter telescope) {
        ParentRef parent = series.getModel().getParent();
        if (parent != null) {
            ModelWithPayload<Brand> brand = commonBrandKeyToCommonBrandMap.get(
                    duplicatedBrandToCommonKeyMap.get(parent.getUri())
            );
            if (brand != null) {
                series.getModel().setParent(brand.getModel());
            }
            String brandUri = series.getModel().getParent().getUri();
            if (!seenContainer.containsKey(brandUri)) {
                cached.put(brandUri, series);
                return;
            }
        }
        writeSeries(series, telescope);
    }

    public void writeSeries(ModelWithPayload<Series> series, OwlTelescopeReporter telescope) {
        String seriesUri = series.getModel().getCanonicalUri();
        writer.createOrUpdate(series.getModel());

        reportContentWithPayloadToTelescope(
                series,
                EntityType.SERIES,
                "Atlas did not return an id after attempting to create or update this Series",
                telescope
        );

        seenContainer.put(seriesUri, series);
        for (ModelWithPayload<? extends Content> episode : cached.removeAll(seriesUri)) {
            write(episode, telescope);
        }
    }

    private void cacheOrWriteItem(
            ModelWithPayload<Content> content,
            OwlTelescopeReporter telescope) {
        ModelWithPayload<Item> item = content.asModelType(Item.class);
        ParentRef parent = item.getModel().getContainer();
        if (parent != null && !seenContainer.containsKey(parent.getUri())) {
            cached.put(parent.getUri(), item);
        } else {
            writer.createOrUpdate(content.asModelType(Item.class).getModel());

            reportContentWithPayloadToTelescope(
                    content,
                    EntityType.CONTENT,
                    "Atlas did not return an id after attempting to create or update this Content",
                    telescope);
        }
    }

    private void cacheOrWriteEpisode(
            ModelWithPayload<Episode> episode,
            OwlTelescopeReporter telescope) {
        ParentRef parent = episode.getModel().getContainer();

        if (parent != null) {
            ModelWithPayload<Brand> brand = commonBrandKeyToCommonBrandMap.get(
                    duplicatedBrandToCommonKeyMap.get(parent.getUri())
            );
            episode.getModel().setContainer(brand.getModel());
            String brandUri = brand.getModel().getCanonicalUri();
            if (!seenContainer.containsKey(brandUri)) {
                cached.put(brandUri, episode);
                return;
            }
        }
        
        String seriesUri = episode.getModel().getSeriesRef() != null ?
                episode.getModel().getSeriesRef().getUri() :
                null;
        if (seriesUri != null) {
            ModelWithPayload<Series> series = commonSeriesKeyToCommonSeriesMap.get(
                    duplicatedSeriesToCommonKeyMap.get(seriesUri)
            );
            if (series != null) {
                episode.getModel().setSeriesRef(ParentRef.parentRefFrom(series.getModel()));
                seriesUri = series.getModel().getCanonicalUri();
            }
            if (!seenContainer.containsKey(seriesUri)) {
                cached.put(seriesUri, episode);
                return;
            }
            episode.getModel().setSeriesNumber(series.getModel().getSeriesNumber());
        }

        writer.createOrUpdate(episode.getModel());

        reportContentWithPayloadToTelescope(
                episode,
                EntityType.EPISODE,
                "Atlas did not return an id after attempting to create or update this Episode",
                telescope);
    }

    private void reportContentWithPayloadToTelescope(
            ModelWithPayload<? extends Content> content,
            EntityType entityType,
            String failedEventMessage,
            OwlTelescopeReporter telescope)
    {
        if(content.getModel().getId() != null){
            telescope.reportSuccessfulEvent(
                    content.getModel().getId(),
                    content.getModel().getAliases(),
                    entityType,
                    content.getPayload()
            );
        } else {
            telescope.reportFailedEvent(
                    failedEventMessage,
                    entityType,
                    content.getPayload()
            );
        }
    }
    private void reportContentToTelescope(
            Content content,
            EntityType entityType,
            String failedEventMessage,
            OwlTelescopeReporter telescope)
    {
        if(content.getId() != null){
            telescope.reportSuccessfulEvent(
                    content.getId(),
                    content.getAliases(),
                    entityType,
                    UNPUBLISH_NO_PAYLOAD_STRING
            );
        } else {
            telescope.reportFailedEvent(
                    failedEventMessage,
                    entityType,
                    UNPUBLISH_NO_PAYLOAD_STRING
            );
        }
    }
}

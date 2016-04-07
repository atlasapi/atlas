package org.atlasapi.remotesite.amazonunbox;

import static com.google.common.base.Preconditions.checkNotNull;

import static com.google.common.base.Predicates.and;
import static com.google.common.base.Predicates.not;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.annotation.Nullable;

import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Film;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.ParentRef;
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

import com.google.common.base.Optional;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Predicate;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.metabroadcast.common.base.Maybe;


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
    
    private static final Predicate<Content> IS_SERIES = new Predicate<Content>() {
        @Override
        public boolean apply(Content input) {
            return input instanceof Series;
        }
    };
    private static final Predicate<Content> IS_ITEM = new Predicate<Content>() {
        @Override
        public boolean apply(Content input) {
            return input instanceof Item;
        }
    };
    public static final String CONTAINER = "CONTAINER";
    public static final String EPISODE = "EPISODE";
    public static final String ITEM = "ITEM";
    public static final String BRAND = "BRAND";
    public static final String SERIES = "SERIES";
    public static final String FILM = "FILM";
    public static final String GB_AMAZON_ASIN = "gb:amazon:asin";

    private final Predicate<Alias> AMAZON_ALIAS = new Predicate<Alias>() {

        @Override
        public boolean apply(Alias input) {
            return GB_AMAZON_ASIN.equals(input.getNamespace());
        }
    };

    private final Logger log = LoggerFactory.getLogger(AmazonUnboxContentWritingItemProcessor.class);
    private final Map<String, Container> seenContainer = Maps.newHashMap();
    private final SetMultimap<String, Content> cached = HashMultimap.create();
    private final Map<String, Brand> topLevelSeries = Maps.newHashMap();
    private final Map<String, Brand> standAloneEpisodes = Maps.newHashMap();
    private final BiMap<String, Content> seenContent = HashBiMap.create();
    private final Map<String, String> linkDuplicatedSeriesUri = Maps.newHashMap();
    private final Map<String, Series> linkSeriesKey = Maps.newHashMap();
    private final Map<String, String> linkDuplicatedBrandUri = Maps.newHashMap();
    private final Map<String, Brand> linkBrandKey = Maps.newHashMap();

    private final ContentExtractor<AmazonUnboxItem, Iterable<Content>> extractor;
    private final ContentResolver resolver;
    private final ContentWriter writer;
    private final ContentLister lister;
    private final int missingContentPercentage;
    private final AmazonUnboxBrandProcessor brandProcessor;
    private final ContentMerger contentMerger;

    public AmazonUnboxContentWritingItemProcessor(ContentExtractor<AmazonUnboxItem, Iterable<Content>> extractor, ContentResolver resolver, 
            ContentWriter writer, ContentLister lister, int missingContentPercentage, AmazonUnboxBrandProcessor brandProcessor) {
        this.extractor = checkNotNull(extractor);
        this.resolver = checkNotNull(resolver);
        this.writer = checkNotNull(writer);
        this.lister = checkNotNull(lister);
        this.missingContentPercentage = checkNotNull(missingContentPercentage);
        this.brandProcessor = checkNotNull(brandProcessor);
        this.contentMerger = new ContentMerger(MergeStrategy.REPLACE, MergeStrategy.REPLACE, MergeStrategy.REPLACE);
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
            checkForDuplicatesSaveForProcessing(content);
        }
    }

    private void checkForDuplicatesSaveForProcessing(Content content) {
        StringBuilder key = new StringBuilder();
        if (content instanceof Container) {

            if (content instanceof Brand) {
                Brand brand = (Brand) content;
                String title = brand.getTitle();
                key.append(title).append(BRAND);
                linkDuplicatedBrandUri.put(brand.getCanonicalUri(), key.toString());
                if (!linkBrandKey.containsKey(key.toString())) {
                    linkBrandKey.put(key.toString(), brand);
                }
            } else if (content instanceof Series){
                Series series = (Series) content;
                String title = series.getTitle();
                String brandTitle = series.getShortDescription();
                key.append(title).append(brandTitle).append(SERIES);
                linkDuplicatedSeriesUri.put(series.getCanonicalUri(), key.toString());
                if (!linkSeriesKey.containsKey(key.toString())) {
                    linkSeriesKey.put(key.toString(), series);
                }
            }
            String hash = key.toString();
            if (!seenContent.containsKey(hash) && !seenContent.containsValue(content)) {
                seenContent.put(hash, content);
            }

        } else {

            if (content instanceof Episode){
                Episode episode = (Episode) content;
                String title = episode.getTitle();
                int episodeNumber = episode.getEpisodeNumber();
                int seasonNumber = episode.getSeriesNumber();
                String brandTitle = episode.getShortDescription();
                key.append(title).append(brandTitle).append(episodeNumber).append(seasonNumber).append(EPISODE);
            } else if (content instanceof Film) {
                Film film = (Film) content;
                String title = film.getTitle();
                Integer year = film.getYear();
                if (film.getYear() != null) {
                    key.append(year);
                }
                key.append(title).append(FILM);
            } else {
                Item item = (Item) content;
                String title = item.getTitle();
                key.append(title).append(ITEM);
            }
            String hash = key.toString();
            if (!seenContent.containsKey(hash)) {
                seenContent.put(hash, content);
            } else {
                Content seen = seenContent.get(hash);
                seenContent.forcePut(hash, mergeAliasesAndLocations(content, seen));
            }
        }
    }

    private Content mergeAliasesAndLocations(Content item, Content seen) {
        Version version = Iterables.getOnlyElement(item.getVersions());
        Alias alias = Iterables.getOnlyElement(Iterables.filter(item.getAliases(), AMAZON_ALIAS));
        version.addAlias(alias);
        seen.addAlias(alias);
        seen.addVersion(version);
        return seen;
    }

    @Override
    public void finish() {
        processSeenContent();
        processTopLevelSeries();
        processStandAloneEpisodes();
        
        if (cached.values().size() > 0) {
            System.out.println(cached.values().size());
            log.warn("{} extracted but unwritten", cached.values().size());
            for (Entry<String, Collection<Content>> mapping : cached.asMap().entrySet()) {
                log.warn(mapping.toString());
            }
        }
        
        seenContainer.clear();
        cached.clear();
        topLevelSeries.clear();
        standAloneEpisodes.clear();
        
        checkForDeletedContent();
        
        seenContent.clear();
    }

    private void processSeenContent() {
        Iterable<Content> brands = Iterables.filter(seenContent.values(), IS_BRAND);
        for (Content container : brands) {
            checkAndWriteSeenContent(container);
        }

        Iterable<Content> series = Iterables.filter(seenContent.values(), IS_SERIES);
        for (Content container : series) {
            checkAndWriteSeenContent(container);
        }

        Iterable<Content> notContainers = Iterables.filter(seenContent.values(), and(not(IS_BRAND), not(IS_SERIES)));
        for (Content notContainer : notContainers) {
            checkAndWriteSeenContent(notContainer);
        }
    }

    private void checkAndWriteSeenContent(Content content) {
        Maybe<Identified> existing = resolve(content.getCanonicalUri());
        if (existing.isNothing()) {
            write(content);
        } else {
            Identified identified = existing.requireValue();
            if (content instanceof Item) {
                write(contentMerger.merge(ContentMerger.asItem(identified), (Item) content));
            } else if (content instanceof Container) {
                write(contentMerger.merge(ContentMerger.asContainer(identified), (Container) content));
            }
        }
    }

    private Predicate<Content> IS_BRAND = new Predicate<Content>() {

        @Override
        public boolean apply(@Nullable Content input) {
            return input instanceof Brand;
        }
    };

    private void processStandAloneEpisodes() {
        for (Entry<String, Brand> entry : standAloneEpisodes.entrySet()) {
            Item item = (Item) Iterables.getOnlyElement(Iterables.filter(cached.get(entry.getKey()), IS_ITEM));
            cached.removeAll(entry.getKey());
            
            if (item instanceof Episode) {
                Episode episode = (Episode) item;
                episode.setSeriesRef(null);
                episode.setSeriesNumber(null);
            }
            item.setParentRef(null);
            write(item);
        }
    }

    private void processTopLevelSeries() {
        for (Entry<String, Brand> entry : topLevelSeries.entrySet()) {
            
            Series series = (Series) Iterables.getOnlyElement(Iterables.filter(cached.get(entry.getKey()), IS_SERIES));
            cached.remove(entry.getKey(), series);
            
            series.setParentRef(null);
            String seriesUri = series.getCanonicalUri();
            // TODO will this update the refs?
            for (Content child : cached.get(seriesUri)) {
                Episode episode = (Episode) child;
                episode.setParentRef(ParentRef.parentRefFrom(series));
                episode.setSeriesRef(null);
                episode.setSeriesNumber(null);
                //write(episode);
            }
            // this will write those items cached against the series
            write(series);
            
            for (Content child : cached.removeAll(entry.getKey())) {
                if (child instanceof Item) {
                    Item item = (Item) child;
                    if (item instanceof Episode) {
                        Episode episode = (Episode) child;
                        episode.setSeriesRef(null);
                        episode.setSeriesNumber(null);
                    } 
                    item.setParentRef(ParentRef.parentRefFrom(series));
                    write(item);
                }
            }
        }
    }
     
    private void checkForDeletedContent() {
        Set<Content> allLoveFilmContent = ImmutableSet.copyOf(resolveAllLoveFilmContent());
        Set<Content> notSeen = Sets.difference(allLoveFilmContent, seenContent.values());
        
//        float missingPercentage = ((float) notSeen.size() / (float) allLoveFilmContent.size()) * 100;
//        if (missingPercentage > (float) missingContentPercentage) {
//            throw new RuntimeException("File failed to update " + missingPercentage + "% of all LoveFilm content. File may be truncated.");
//        } else {
//            // TODO check/test if this does what it should
//            List<Content> orderedContent = REVERSE_HIERARCHICAL_ORDER.sortedCopy(notSeen);
//            for (Content notSeenContent : orderedContent) {
//                notSeenContent.setActivelyPublished(false);
//                // write
//                if (notSeenContent instanceof Item) {
//                    writer.createOrUpdate((Item) notSeenContent);
//                } else if (notSeenContent instanceof Container) {
//                    writer.createOrUpdate((Container) notSeenContent);
//                } else {
//                    throw new RuntimeException("LoveFilm content with uri " + notSeenContent.getCanonicalUri() + " not an Item or a Container");
//                }
//            }
//        }
        //Temporarily commented out check for missingContentPercentage, will uncomment after first ingest with deduping logic
        List<Content> orderedContent = REVERSE_HIERARCHICAL_ORDER.sortedCopy(notSeen);
        for (Content notSeenContent : orderedContent) {
            notSeenContent.setActivelyPublished(false);
            if (notSeenContent instanceof Item) {
                writer.createOrUpdate((Item) notSeenContent);
            } else if (notSeenContent instanceof Container) {
                writer.createOrUpdate((Container) notSeenContent);
            } else {
                throw new RuntimeException("LoveFilm content with uri " + notSeenContent.getCanonicalUri() + " not an Item or a Container");
            }
        }
    }

    private Iterator<Content> resolveAllLoveFilmContent() {
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
    
    private void write(Content content) {
        if (content instanceof Container) {
            if (content instanceof Series) {
                cacheOrWriteSeriesAndSubContents((Series) content);
            } else if (content instanceof Brand) {
                cacheOrWriteBrandAndCachedSubContents((Brand) content);
            } else {
                writer.createOrUpdate((Container) content);
            }
        } else if (content instanceof Item) {
            if (content instanceof Episode) {
                cacheOrWriteEpisode((Episode) content);
            } else {
                cacheOrWriteItem(content);
            }
        }
    }

    private void cacheOrWriteBrandAndCachedSubContents(Brand brand) {
        String brandUri = brand.getCanonicalUri();
        BrandType brandType = brandProcessor.getBrandType(brandUri);
        if (BrandType.TOP_LEVEL_SERIES.equals(brandType)) {
            log.trace("Caching top-level series " + brandUri);
            topLevelSeries.put(brandUri, brand);
        } else if (BrandType.STAND_ALONE_EPISODE.equals(brandType)) {
            log.trace("Caching stand-alone episode " + brandUri);
            standAloneEpisodes.put(brandUri, brand);
        } else {
            writeBrand(brand);
        }
    }
    
    private void writeBrand(Brand brand) {
        writer.createOrUpdate(brand);
        String brandUri = brand.getCanonicalUri();
        seenContainer.put(brandUri, brand);
        for (Content subContent : cached.removeAll(brandUri)) {
            write(subContent);
        }
    }

    private void cacheOrWriteSeriesAndSubContents(Series series) {
        ParentRef parent = series.getParent();
        if (parent != null && !seenContainer.containsKey(parent.getUri())) {
            cached.put(parent.getUri(), series);
        } else {
            writeSeries(series);
        }
    }

    public void writeSeries(Series series) {
        String seriesUri = series.getCanonicalUri();
        writer.createOrUpdate(series);
        seenContainer.put(seriesUri, series);
        for (Content episode : cached.removeAll(seriesUri)) {
            write(episode);
        }
    }

    private void cacheOrWriteItem(Content content) {
        Item item = (Item) content;
        ParentRef parent = item.getContainer();
        if (parent != null && !seenContainer.containsKey(parent.getUri())) {
            cached.put(parent.getUri(), item);
        } else {
            writer.createOrUpdate((Item) content);
        }
    }

    private void cacheOrWriteEpisode(Episode episode) {
        ParentRef parent = episode.getContainer();

        if (parent != null) {
            Brand brand = linkBrandKey.get(linkDuplicatedBrandUri.get(parent.getUri()));
            episode.setContainer(brand);
            String brandUri = brand.getCanonicalUri();
            if (!seenContainer.containsKey(brandUri)) {
                cached.put(brandUri, episode);
                return;
            }
        }
        
        String seriesUri = episode.getSeriesRef() != null ? episode.getSeriesRef().getUri() : null;
        if (seriesUri != null) {
            Series series = linkSeriesKey.get(linkDuplicatedSeriesUri.get(seriesUri));
            if (series != null) {
                episode.setSeriesRef(ParentRef.parentRefFrom(series));
                seriesUri = series.getCanonicalUri();
            }
            if (!seenContainer.containsKey(seriesUri)) {
                cached.put(seriesUri, episode);
                return;
            }
            episode.setSeriesNumber(series.getSeriesNumber());
        }

        writer.createOrUpdate(episode);
    }
}

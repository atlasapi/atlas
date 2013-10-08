package org.atlasapi.remotesite.amazonunbox;

import static org.atlasapi.feeds.youview.YouViewDeleter.REVERSE_HIERARCHICAL_ORDER;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.ParentRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Series;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.listing.ContentLister;
import org.atlasapi.persistence.content.listing.ContentListingCriteria;
import org.atlasapi.remotesite.ContentExtractor;
import org.atlasapi.remotesite.ContentMerger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.metabroadcast.common.base.Maybe;


public class AmazonUnboxProcessingItemProcessor implements AmazonUnboxItemProcessor {
    
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
    
    private final Logger log = LoggerFactory.getLogger(AmazonUnboxProcessingItemProcessor.class);
    private final Map<String, Container> seen = Maps.newHashMap();
    private final SetMultimap<String, Content> cached = HashMultimap.create();
    private final Map<String, Brand> topLevelSeries = Maps.newHashMap();
    private final Map<String, Brand> standAloneEpisodes = Maps.newHashMap();
    private final Set<Content> seenContent = Sets.newHashSet();

    private final ContentExtractor<AmazonUnboxItem, Optional<Content>> extractor;
    private final ContentResolver resolver;
    private final ContentWriter writer;
    private final ContentLister lister;
    private final int missingContentPercentage;
    private final AmazonUnboxBrandProcessor brandProcessor;

    public AmazonUnboxProcessingItemProcessor(ContentExtractor<AmazonUnboxItem, Optional<Content>> extractor, ContentResolver resolver, 
            ContentWriter writer, ContentLister lister, int missingContentPercentage, AmazonUnboxBrandProcessor brandProcessor) {
        this.extractor = extractor;
        this.resolver = resolver;
        this.writer = writer;
        this.lister = lister;
        this.missingContentPercentage = missingContentPercentage;
        this.brandProcessor = brandProcessor;
    }

    @Override
    public void prepare() {
        seen.clear();
        cached.clear();
        topLevelSeries.clear();
        standAloneEpisodes.clear();
        seenContent.clear();
    }
    
    @Override
    public void process(AmazonUnboxItem item) {
        Optional<Content> possibleContent = extract(item);
        if (!possibleContent.isPresent()) {
            return;
        }
        Content content = possibleContent.get();
        seenContent.add(content);
        Maybe<Identified> existing = resolve(content.getCanonicalUri());
        if (existing.isNothing()) {
            write(content);
        } else {
            Identified identified = existing.requireValue();
            if (content instanceof Item) {
                write(ContentMerger.merge(ContentMerger.asItem(identified), (Item) content));
            } else if (content instanceof Container) {
                write(ContentMerger.merge(ContentMerger.asContainer(identified), (Container) content));
            }
        }
    }
    
    @Override
    public void finish() {
        processTopLevelSeries();
        processStandAloneEpisodes();
        
        if (cached.values().size() > 0) {
            log.warn("{} extracted but unwritten", cached.values().size());
            for (Entry<String, Collection<Content>> mapping : cached.asMap().entrySet()) {
                log.warn(mapping.toString());
            }
        }
        
        seen.clear();
        cached.clear();
        topLevelSeries.clear();
        standAloneEpisodes.clear();
        
        checkForDeletedContent();
        
        seenContent.clear();
    }

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
        Set<Content> notSeen = Sets.difference(allLoveFilmContent, seenContent);
        
        float missingPercentage = ((float) notSeen.size() / (float) allLoveFilmContent.size()) * 100;
        if (missingPercentage > (float) missingContentPercentage) {
            throw new RuntimeException("File failed to update " + missingPercentage + "% of all LoveFilm content. File may be truncated.");
        } else {
            // TODO check/test if this does what it should
            List<Content> orderedContent = REVERSE_HIERARCHICAL_ORDER.sortedCopy(notSeen);
            for (Content notSeenContent : orderedContent) {
                notSeenContent.setActivelyPublished(false);
                // write
                if (notSeenContent instanceof Item) {
                    writer.createOrUpdate((Item) notSeenContent);
                } else if (notSeenContent instanceof Container) {
                    writer.createOrUpdate((Container) notSeenContent);
                } else {
                    throw new RuntimeException("LoveFilm content with uri " + notSeenContent.getCanonicalUri() + " not an Item or a Container");
                }
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
    
    public Optional<Content> extract(AmazonUnboxItem item) {
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
        
        if (brandType.equals(BrandType.TOP_LEVEL_SERIES)) {
            topLevelSeries.put(brandUri, brand);
        } else if (brandType.equals(BrandType.STAND_ALONE_EPISODE)) {
            standAloneEpisodes.put(brandUri, brand);
        } else {
            writeBrand(brand);
        }
    }
    
    private void writeBrand(Brand brand) {
        writer.createOrUpdate(brand);
        String brandUri = brand.getCanonicalUri();
        seen.put(brandUri, brand);
        for (Content subContent : cached.removeAll(brandUri)) {
            write(subContent);
        }
    }

    private void cacheOrWriteSeriesAndSubContents(Series series) {
        ParentRef parent = series.getParent();
        if (parent != null && !seen.containsKey(parent.getUri())) {
            cached.put(parent.getUri(), series);
        } else {
            writeSeries(series);
        }
    }

    public void writeSeries(Series series) {
        String seriesUri = series.getCanonicalUri();
        writer.createOrUpdate(series);
        seen.put(seriesUri, series);
        for (Content episode : cached.removeAll(seriesUri)) {
            write(episode);
        }
    }

    private void cacheOrWriteItem(Content content) {
        Item item = (Item) content;
        ParentRef parent = item.getContainer();
        if (parent != null && !seen.containsKey(parent.getUri())) {
            cached.put(parent.getUri(), item);
        } else {
            writer.createOrUpdate((Item) content);
        }
    }

    private void cacheOrWriteEpisode(Episode episode) {
        ParentRef parent = episode.getContainer();
        
        if (parent != null && !seen.containsKey(parent.getUri())) {
            cached.put(parent.getUri(), episode);
            return;
        } 
        
        String seriesUri = episode.getSeriesRef() != null ? episode.getSeriesRef().getUri() : null;
        if (seriesUri != null) {
            if (!seen.containsKey(seriesUri)) {
                cached.put(seriesUri, episode);
                return;
            }
            Series series = (Series) seen.get(seriesUri);
            episode.setSeriesNumber(series.getSeriesNumber());
        }
        
        writer.createOrUpdate(episode);
    }
}

package org.atlasapi.remotesite.lovefilm;

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
import org.atlasapi.remotesite.ContentMerger.MergeStrategy;
import org.atlasapi.remotesite.lovefilm.LoveFilmData.LoveFilmDataRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.metabroadcast.common.base.Maybe;

/**
 * Extracts LoveFilmDataRow, merges with existing Content and writes.
 * 
 * Rows are not required to be passed to this in the order they have to be
 * written. To achieve this a set of 'seen' content IDs is maintained along with
 * a Multimap of content ID to cached Content. When Content is extracted, if its
 * parent(s) have not yet been written it is placed in the cache mapped against
 * the missing parent. When a possible parent is written its ID is added to
 * 'seen' and any Content cached against that ID is written and removed from the
 * cache.
 * 
 */
public class DefaultLoveFilmDataRowHandler implements LoveFilmDataRowHandler {
    
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

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final ContentResolver resolver;
    private final ContentWriter writer;
    private final ContentLister lister;
    private final ContentExtractor<LoveFilmDataRow, Optional<Content>> extractor;

    private final Map<String, Container> seen = Maps.newHashMap();
    private final SetMultimap<String, Content> cached = HashMultimap.create();
    private final Map<String, Brand> unwrittenBrands = Maps.newHashMap();
    private final Set<Content> seenContent = Sets.newHashSet();
    private final int missingContentPercentage;
    private final ContentMerger contentMerger;
    
    public DefaultLoveFilmDataRowHandler(ContentResolver resolver, ContentWriter writer, ContentLister lister, int missingContentPercentage) {
        this(resolver, writer, lister, new LoveFilmDataRowContentExtractor(), missingContentPercentage);
    }

    public DefaultLoveFilmDataRowHandler(ContentResolver resolver, ContentWriter writer, ContentLister lister, 
            ContentExtractor<LoveFilmDataRow, Optional<Content>> extractor, int missingContentPercentage) {
        this.resolver = resolver;
        this.writer = writer;
        this.lister = lister;
        this.extractor = extractor;
        this.missingContentPercentage = missingContentPercentage;
        this.contentMerger = new ContentMerger(MergeStrategy.MERGE, MergeStrategy.KEEP, MergeStrategy.MERGE);
    }

    @Override
    public void prepare() {
    }

    @Override
    public void handle(LoveFilmDataRow row) {
        Optional<Content> possibleContent = extract(row);
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
                write(contentMerger.merge(ContentMerger.asItem(identified), (Item) content));
            } else if (content instanceof Container) {
                write(contentMerger.merge(ContentMerger.asContainer(identified), (Container) content));
            }
        }
    }

    @Override
    public void finish() {
        if (!unwrittenBrands.isEmpty()) {
            processTopLevelSeries();
        }
        
        if (cached.values().size() > 0) {
            log.warn("{} extracted but unwritten", cached.values().size());
            for (Entry<String, Collection<Content>> mapping : cached.asMap().entrySet()) {
                log.warn(mapping.toString());
            }

        }
        
        seen.clear();
        cached.clear();
        unwrittenBrands.clear();
        
        checkForDeletedContent();
        
        seenContent.clear();
    }

    private void processTopLevelSeries() {
        for (Entry<String, Brand> entry : unwrittenBrands.entrySet()) {
            Iterable<Content> seriesForBrand = Iterables.filter(cached.get(entry.getKey()), IS_SERIES);
            if (mightBeTopLevelSeries(entry.getValue(), seriesForBrand)) {
                
                seen.put(entry.getKey(), entry.getValue());
                
                Series series = (Series) Iterables.getOnlyElement(seriesForBrand);
                cached.remove(entry.getKey(), series);
                series.setParentRef(null);
                String seriesUri = series.getCanonicalUri();
                write(series);
                for (Content child : cached.removeAll(seriesUri)) {
                    Episode episode = (Episode) child;
                    episode.setParentRef(ParentRef.parentRefFrom(series));
                    episode.setSeriesRef(null);
                    episode.setSeriesNumber(null);
                    write(episode);
                }
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
            } else {
                writeBrand(entry.getValue());
            }
        }
    }
    
    /*
     * This check can only truely check whether a brand and children constitutes a top-level series
     * once all information is known. When only some rows have been parsed, the check can only be
     * used to reject a potential top-level series
     * 
     * If a brand has two or more series, than it can be said to be non-topLevel, whereas if it only 
     * has one, then you can only say that it is not a top-level series if the brand name and series 
     * name do not match. if they do, you cannot assert that it is a top-level series, as another series
     * may yet be ingested for that brand.  
     */
    private boolean mightBeTopLevelSeries(Brand brand, Iterable<Content> seriesForBrand) {
        if (Iterables.size(seriesForBrand) == 1) {
            Content series = Iterables.getOnlyElement(seriesForBrand);
            return series.getTitle().equals(brand.getTitle());
        }
        return false;
    }
    
    private void checkForDeletedContent() {
        Set<Content> allLoveFilmContent = ImmutableSet.copyOf(resolveAllLoveFilmContent());
        Set<Content> notSeen = Sets.difference(allLoveFilmContent, seenContent);
        
        float missingPercentage = ((float) notSeen.size() / (float) allLoveFilmContent.size()) * 100;
        if (missingPercentage > (float) missingContentPercentage) {
            throw new RuntimeException("File failed to update " + missingPercentage + "% of all LoveFilm content. File may be truncated.");
        } else {
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
            .forPublisher(Publisher.LOVEFILM)
            .build();
        
        return lister.listContent(criteria);
    }

    private Optional<Content> extract(LoveFilmDataRow row) {
        return extractor.extract(row);
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
        Iterable<Content> cachedSeriesForBrand = Iterables.filter(cached.get(brandUri), IS_SERIES);
        if (!mightBeTopLevelSeries(brand, cachedSeriesForBrand)) {
            writeBrand(brand);
        } else {
            unwrittenBrands.put(brandUri, brand);
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
            // series has parent, parent not written
            if (unwrittenBrands.containsKey(parent.getUri())) {
                Brand brand = unwrittenBrands.get(parent.getUri());
                Iterable<Content> cachedSeriesForBrand = Iterables.filter(cached.get(parent.getUri()), IS_SERIES);
                if (!mightBeTopLevelSeries(brand, Iterables.concat(cachedSeriesForBrand, ImmutableList.of(series)))) {
                    // have brand, is now definitely not a top level series, so can write brand, etc
                    unwrittenBrands.remove(parent.getUri());
                    writeBrand(brand);
                    writeSeries(series);
                    return;
                } 
            }
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
        String brandUri = episode.getContainer().getUri();
        
        if (!seen.containsKey(brandUri)) {
            cached.put(brandUri, episode);
            return;
        } 
        
        String seriesUri = episode.getSeriesRef() != null ? episode.getSeriesRef().getUri() : null;
        if (seriesUri != null) {
            if (!seen.containsKey(seriesUri)) {
                cached.put(seriesUri, episode);
                return;
            }
            Series series = (Series)seen.get(seriesUri);
            episode.setSeriesNumber(series.getSeriesNumber());
        }
        
        writer.createOrUpdate(episode);
    }
}

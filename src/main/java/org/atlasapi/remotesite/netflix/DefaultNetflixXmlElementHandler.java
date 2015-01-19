package org.atlasapi.remotesite.netflix;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import nu.xom.Element;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.ParentRef;
import org.atlasapi.media.entity.Series;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.remotesite.ContentExtractor;
import org.atlasapi.remotesite.ContentMerger;
import org.atlasapi.remotesite.ContentMerger.MergeStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;
import com.metabroadcast.common.base.Maybe;

public class DefaultNetflixXmlElementHandler implements NetflixXmlElementHandler {

    private final Map<String, Container> seen = Maps.newHashMap();
    private final ContentExtractor<Element, Set<? extends Content>> extractor;
    private final SetMultimap<String, Content> cached = HashMultimap.create();
    private final ContentWriter writer;
    private final ContentResolver resolver;
    private final Logger log = LoggerFactory.getLogger(DefaultNetflixXmlElementHandler.class);
    private final ContentMerger contentMerger;
    
    public DefaultNetflixXmlElementHandler(ContentExtractor<Element, Set<? extends Content>> extractor, ContentResolver resolver, ContentWriter writer) {
        this.extractor = extractor;
        this.resolver = resolver;
        this.writer = writer;
        this.contentMerger = new ContentMerger(MergeStrategy.MERGE, MergeStrategy.KEEP, MergeStrategy.REPLACE);
    }

    @Override
    public void prepare() {
    }

    @Override
    public void handle(Element element) {
        for (Content content : extractor.extract(element)) {
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
    }

    @Override
    public void finish() {
        if (cached.values().size() > 0) {
            log.warn("{} extracted but unwritten", cached.values().size());
            for (Entry<String, Collection<Content>> mapping : cached.asMap().entrySet()) {
                log.warn(mapping.toString());
            }

        }
        seen.clear();
        cached.clear();
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
                writeBrandAndCachedSubContents((Brand) content);
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

    private void writeBrandAndCachedSubContents(Brand brand) {
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
            String seriesUri = series.getCanonicalUri();
            writer.createOrUpdate(series);
            seen.put(seriesUri, series);
            for (Content episode : cached.removeAll(seriesUri)) {
                write(episode);
            }
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

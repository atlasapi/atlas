package org.atlasapi.remotesite.itv.interlinking;

import static org.atlasapi.remotesite.itv.interlinking.ItvInterlinkingContentExtractor.INTERLINKING_NS;
import static org.atlasapi.remotesite.itv.interlinking.XomElement.requireElemValue;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import nu.xom.Element;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.Described;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Series;
import org.atlasapi.media.entity.Version;
import org.atlasapi.persistence.content.ContentWriter;
import org.joda.time.Duration;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.metabroadcast.common.base.Maybe;

public class ItvInterlinkingEntryProcessor {

    private final Logger log = LoggerFactory.getLogger(ItvInterlinkingEntryProcessor.class);
    private final Map<String, InterlinkingEntry<? extends Identified>> idToContent = Maps.newHashMap();
    private final Multimap<String, InterlinkingEntry<? extends Identified>> parentIdToContent = ArrayListMultimap.create();
    private final Set<InterlinkingEntry<? extends Identified>> parentless = Sets.newHashSet();
    
    private final ContentWriter contentWriter;
    private final ItvInterlinkingContentExtractor extractor;
    
    public ItvInterlinkingEntryProcessor(ContentWriter contentWriter, ItvInterlinkingContentExtractor contentExtractor) {
        this.contentWriter = contentWriter;
        this.extractor = contentExtractor;
    }
    
    public void processEntry(Element entryElem) {
        try {
            InterlinkingType type = InterlinkingType.fromKey(requireElemValue(entryElem, "type", INTERLINKING_NS));
            InterlinkingOperation operation = InterlinkingOperation.valueOf(requireElemValue(entryElem, "operation", INTERLINKING_NS).toUpperCase());

            if (operation == InterlinkingOperation.STORE) {
                InterlinkingEntry<? extends Identified> content = null;
                if (type == InterlinkingType.BRAND) {
                    content = extractor.getBrand(entryElem);
                } else if (type == InterlinkingType.EPISODE) {
                    content = extractor.getEpisode(entryElem);
                } else if (type == InterlinkingType.BROADCAST) {
                    content = extractor.getBroadcast(entryElem);
                } else if (type == InterlinkingType.ONDEMAND) {
                    content = extractor.getOnDemand(entryElem);
                } else if (type == InterlinkingType.SERIES) {
                    content = extractor.getSeries(entryElem);
                } else if (type == InterlinkingType.SUBSERIES) {
                    throw new RuntimeException("Subseries found");
                } else {
                    throw new RuntimeException("unknown type " + type);
                }

                if (content != null) {
                    idToContent.put(content.getId(), content);
                    if (content.getParentId().hasValue()) {
                        parentIdToContent.put(content.getParentId().requireValue(), content);
                    } else {
                        parentless.add(content);
                    }
                }
            }
        } catch (NullPointerException e) {
            log.warn("Element {} thrown exception {}", entryElem, e);
        }

    }
    
    public void processAllEntries() {
        for (InterlinkingEntry<? extends Identified> entry : parentless) {
            if (entry.getContent() instanceof Item) {
                processItemChildren((InterlinkingEntry<Item>)entry);
            } else if (entry.getContent() instanceof Brand) {
                processBrandChildren((InterlinkingEntry<Brand>)entry);
            } else if (entry.getContent() instanceof Series) {
                processSeriesChildren((InterlinkingEntry<Series>)entry, Maybe.<InterlinkingEntry<Brand>>nothing());
            }
        }
    }
    
    private void processSeriesChildren(InterlinkingEntry<Series> series, Maybe<InterlinkingEntry<Brand>> brand) {
        contentWriter.createOrUpdate(series.getContent());
        
        Collection<InterlinkingEntry<? extends Identified>> children = parentIdToContent.get(series.getId());
        for (InterlinkingEntry<? extends Identified> child : children) {
            if (child.getContent() instanceof Item) {
                Item item;
                if (child.getContent() instanceof Episode) {
                    Episode episode = (Episode) child.getContent();
                    item = episode;
                    
                    episode.setSeries(series.getContent());
                    if (series.getIndex().hasValue()) {
                        episode.setSeriesNumber(series.getIndex().requireValue());
                    }
                } else {
                    item = (Item) child.getContent();
                }
                
                if (brand.hasValue()) {
                    item.setContainer(brand.requireValue().getContent());
                } else {
                    item.setContainer(series.getContent());
                }
                processItemChildren((InterlinkingEntry<Item>)child);
            } else {
                throw new RuntimeException("Series cannot have a child of type " + child.getContent().getClass());
            }
        }
    }
    
    private void processItemChildren(InterlinkingEntry<Item> item) {
        Collection<InterlinkingEntry<? extends Identified>> children = parentIdToContent.get(item.getId());
        
        Version version;
        if (item.getContent().getVersions().isEmpty()) {
            version = new Version();
        } else {
            version = Iterables.getOnlyElement(item.getContent().getVersions());
        }
        
        for (InterlinkingEntry<? extends Identified> child : children) {
            if (child.getContent() instanceof Broadcast) {
                version.addBroadcast((Broadcast)child.getContent());
            } else if (child.getContent() instanceof Version) {
                Version childVersion = (Version) child.getContent();
                copyVersionAttributes(childVersion, version);
                setLinkLocation(item.getLink().requireValue(), childVersion);
                version.setManifestedAs(childVersion.getManifestedAs());
            } 
        }
        
        ((Item) item.getContent()).setVersions(ImmutableSet.of(version));
        
        contentWriter.createOrUpdate(item.getContent());
    }
    
    private void setLinkLocation(String link, Version version) {
        Iterables.getOnlyElement(Iterables.getOnlyElement(version.getManifestedAs()).getAvailableAt()).setUri(link);
    }
    
    private void copyVersionAttributes(Version from, Version to) {
        to.setDuration(Duration.standardSeconds(from.getDuration()));
        to.setProvider(from.getProvider());
    }
    
    private void processBrandChildren(InterlinkingEntry<Brand> brand) {

        contentWriter.createOrUpdate(brand.getContent());
        
        Collection<InterlinkingEntry<? extends Identified>> collection = parentIdToContent.get(brand.getId());
        for (InterlinkingEntry<? extends Identified> child : collection) {
            if (child.getContent() instanceof Item) {
                Item item = (Item) child.getContent();
                item.setContainer(brand.getContent());
                inherit(brand.getContent(), item);
                
                processItemChildren((InterlinkingEntry<Item>) child);
                
            } else if (child.getContent() instanceof Series) {
                Series series = (Series) child.getContent();
                series.setParent(brand.getContent());
                inherit(brand.getContent(), series);
                
                processSeriesChildren((InterlinkingEntry<Series>) child, Maybe.just(brand));
                
            } else {
                throw new RuntimeException("Child of brand cannot be " + child.getContent().getClass());
            }
        }
    }
    
    private void inherit(Described parent, Described child) {
        if (child.getThumbnail() == null) {
            child.setThumbnail(parent.getThumbnail());
        }
        if (child.getGenres().isEmpty()) {
            child.setGenres(parent.getGenres());
        }
        if (child.getTags().isEmpty()) {
            child.setTags(parent.getTags());
        }
    }
}

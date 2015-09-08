package org.atlasapi.remotesite.pa;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Series;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.persistence.content.people.ItemsPeopleWriter;
import org.atlasapi.remotesite.channel4.pmlsd.epg.ContentHierarchyAndBroadcast;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;


/**
 * Maintain a buffer of content to write, which acts as a write-through caching implementation
 * of a {@link ContentResolver}
 * 
 * @author tom
 *
 */
public class ContentBuffer implements ContentResolver {

    private static final Logger log = LoggerFactory.getLogger(ContentBuffer.class);
    
    private static ThreadLocal<Map<String, Identified>> contentCache = new ThreadLocal<Map<String, Identified>>() {
        
        @Override 
        protected Map<String, Identified> initialValue() {
            return Maps.newHashMap();
        }
    };
    
    private static ThreadLocal<List<ContentHierarchyAndBroadcast>> hierarchies = new ThreadLocal<List<ContentHierarchyAndBroadcast>>() {
        
        @Override
        protected List<ContentHierarchyAndBroadcast> initialValue() {
            return Lists.newArrayList();
        }
    };

    private final ContentResolver resolver;
    private final ContentWriter writer;
    private final ItemsPeopleWriter peopleWriter;
    
    public ContentBuffer(ContentResolver contentResolver, ContentWriter contentWriter, ItemsPeopleWriter peopleWriter) {
        this.resolver = checkNotNull(contentResolver);
        this.writer = checkNotNull(contentWriter);
        this.peopleWriter = checkNotNull(peopleWriter);
    }
    
    public void add(ContentHierarchyAndBroadcast hierarchy) {
        if (hierarchy.getBrand().isPresent()) {
            contentCache.get().put(hierarchy.getBrand().get().getCanonicalUri(), hierarchy.getBrand().get());
        }
        if (hierarchy.getSeries().isPresent()) {
            contentCache.get().put(hierarchy.getSeries().get().getCanonicalUri(), hierarchy.getSeries().get());
        }
        contentCache.get().put(hierarchy.getItem().getCanonicalUri(), hierarchy.getItem());
        hierarchies.get().add(hierarchy);
    }

    @Override
    public ResolvedContent findByCanonicalUris(Iterable<? extends String> canonicalUris) {
        Identified identified = contentCache.get().get(Iterables.getOnlyElement(canonicalUris));

        if (identified != null) {
            return ResolvedContent.builder().put(identified.getCanonicalUri(), identified).build();
        }
        return resolver.findByCanonicalUris(canonicalUris);
    }

    @Override
    public ResolvedContent findByUris(Iterable<String> uris) {
        throw new UnsupportedOperationException();
    }
    
    public void flush() {
        Set<Identified> written = Sets.newHashSet();
        try {
            for(ContentHierarchyAndBroadcast hierarchy : ImmutableList.copyOf(hierarchies.get()).reverse()) {
                try {
                    process(hierarchy, written);
                } catch (Exception e) {
                    log.error(String.format("Failed writing item %s, broadcast %s on %s", 
                                hierarchy.getItem().getCanonicalUri(),
                                hierarchy.getBroadcast().getTransmissionTime(), 
                                hierarchy.getBroadcast().getBroadcastOn()), e);
                }
            }
        } finally {
            hierarchies.get().clear();
            contentCache.get().clear();
        }
    }

    private void process(ContentHierarchyAndBroadcast hierarchy, Set<Identified> alreadyWritten) {
        if (hierarchy.getBrand().isPresent()) {
            Brand brand = hierarchy.getBrand().get();
            if (!alreadyWritten.contains(brand)) {
                writer.createOrUpdate(brand);
                alreadyWritten.add(brand);
            }
            hierarchy.getItem().setContainer(brand);
        }
        
        if (hierarchy.getSeries().isPresent()) {
            Series series = hierarchy.getSeries().get();
            if (!alreadyWritten.contains(series)) {
                if (hierarchy.getBrand().isPresent()) {
                    series.setParent(hierarchy.getBrand().get());
                }
                writer.createOrUpdate(series);
                alreadyWritten.add(series);
            }
            
            if (!hierarchy.getBrand().isPresent()) {
                hierarchy.getItem().setContainer(series);
            } else {
                if (hierarchy.getItem() instanceof Episode) {
                    ((Episode) hierarchy.getItem()).setSeries(series);
                }
            }
        }
        
        if (!alreadyWritten.contains(hierarchy.getItem())) {
            Item item = hierarchy.getItem();
            
            writer.createOrUpdate(item);
            peopleWriter.createOrUpdatePeople(item);
            alreadyWritten.add(item);
        }
    }
    
}

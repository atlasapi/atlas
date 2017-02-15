package org.atlasapi.remotesite.pa;

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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Maintain a buffer of content to write, which acts as a write-through caching implementation
 * of a {@link ContentResolver}
 */
public class ContentBuffer implements ContentResolver {

    private static final Logger log = LoggerFactory.getLogger(ContentBuffer.class);

    private static ThreadLocal<Map<String, Identified>> contentCache =
            ThreadLocal.withInitial(Maps::newHashMap);

    private static ThreadLocal<Map<String, String>> aliasToCanonicalUri =
            ThreadLocal.withInitial(Maps::newHashMap);

    private static ThreadLocal<List<ContentHierarchyAndSummaries>> hierarchies =
            ThreadLocal.withInitial(Lists::newArrayList);

    private final ContentResolver resolver;
    private final ContentWriter writer;
    private final ItemsPeopleWriter peopleWriter;
    
    private ContentBuffer(
            ContentResolver contentResolver,
            ContentWriter contentWriter,
            ItemsPeopleWriter peopleWriter
    ) {
        this.resolver = checkNotNull(contentResolver);
        this.writer = checkNotNull(contentWriter);
        this.peopleWriter = checkNotNull(peopleWriter);
    }

    public static ContentBuffer create(
            ContentResolver contentResolver,
            ContentWriter contentWriter,
            ItemsPeopleWriter peopleWriter
    ) {
        return new ContentBuffer(contentResolver, contentWriter, peopleWriter);
    }

    public void add(ContentHierarchyAndSummaries hierarchy) {
        if (hierarchy.getBrand().isPresent()) {
            contentCache.get().put(
                    hierarchy.getBrand().get().getCanonicalUri(),
                    hierarchy.getBrand().get()
            );
        }

        if (hierarchy.getSeries().isPresent()) {
            contentCache.get().put(
                    hierarchy.getSeries().get().getCanonicalUri(),
                    hierarchy.getSeries().get()
            );
        }

        contentCache.get().put(
                hierarchy.getItem().getCanonicalUri(),
                hierarchy.getItem()
        );

        for (String aliasUrl : hierarchy.getItem().getAliasUrls()) {
            aliasToCanonicalUri.get().put(aliasUrl, hierarchy.getItem().getCanonicalUri());
        }

        if (hierarchy.getBrandSummary().isPresent()) {
            contentCache.get().put(
                    hierarchy.getBrandSummary().get().getCanonicalUri(),
                    hierarchy.getBrandSummary().get()
            );
        }

        if (hierarchy.getSeriesSummary().isPresent()) {
            contentCache.get().put(
                    hierarchy.getSeriesSummary().get().getCanonicalUri(),
                    hierarchy.getSeriesSummary().get()
            );
        }

        hierarchies.get().add(hierarchy);
    }

    @Override
    public ResolvedContent findByCanonicalUris(Iterable<String> canonicalUris) {
        Identified identified = contentCache.get().get(Iterables.getOnlyElement(canonicalUris));
        
        if (identified != null) {
            return ResolvedContent.builder().put(identified.getCanonicalUri(), identified).build();
        }
        return resolver.findByCanonicalUris(canonicalUris);
    }

    @Override
    public ResolvedContent findByUris(Iterable<String> uris) {
        for (String uri : uris) {

            Identified identified = contentCache.get().get(uri);

            if (identified != null) {
                return ResolvedContent.builder().put(uri, identified).build();
            }

            String canonicalUri = aliasToCanonicalUri.get().get(uri);
            if (canonicalUri != null) {
                identified = contentCache.get().get(canonicalUri);
                if (identified != null) {
                    return ResolvedContent.builder().put(canonicalUri, identified).build();
                }
            }
        }
        return resolver.findByUris(uris);
    }

    public void flush() {
        Set<Identified> written = Sets.newHashSet();

        try {
            for(ContentHierarchyAndSummaries hierarchy
                    : ImmutableList.copyOf(hierarchies.get()).reverse()) {
                try {
                    write(hierarchy, written);
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
            aliasToCanonicalUri.get().clear();
        }
    }

    private void write(ContentHierarchyAndSummaries hierarchy, Set<Identified> alreadyWritten) {
        if (hierarchy.getBrand().isPresent()) {
            writeBrand(hierarchy, alreadyWritten);
        }
        
        if (hierarchy.getSeries().isPresent()) {
            writeSeries(hierarchy, alreadyWritten);
        }

        writeItem(hierarchy, alreadyWritten);

        if (hierarchy.getBrandSummary().isPresent()) {
            writeBrandSummary(hierarchy, alreadyWritten);
        }

        if (hierarchy.getSeriesSummary().isPresent()) {
            writeSeriesSummary(hierarchy, alreadyWritten);
        }
    }

    private void writeBrand(
            ContentHierarchyAndSummaries hierarchy,
            Set<Identified> alreadyWritten
    ) {
        Brand brand = hierarchy.getBrand().get();

        if (!alreadyWritten.contains(brand)) {
            writer.createOrUpdate(brand);
            alreadyWritten.add(brand);
        }

        hierarchy.getItem().setContainer(brand);
    }

    private void writeSeries(
            ContentHierarchyAndSummaries hierarchy,
            Set<Identified> alreadyWritten
    ) {
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

    private void writeItem(
            ContentHierarchyAndSummaries hierarchy,
            Set<Identified> alreadyWritten
    ) {
        if (!alreadyWritten.contains(hierarchy.getItem())) {
            Item item = hierarchy.getItem();

            writer.createOrUpdate(item);
            peopleWriter.createOrUpdatePeople(item);
            alreadyWritten.add(item);
        }
    }

    private void writeBrandSummary(
            ContentHierarchyAndSummaries hierarchy,
            Set<Identified> alreadyWritten
    ) {
        Brand brandSummary = hierarchy.getBrandSummary().get();

        if (!alreadyWritten.contains(brandSummary)) {
            writer.createOrUpdate(brandSummary);
            alreadyWritten.add(brandSummary);
        }
    }

    private void writeSeriesSummary(
            ContentHierarchyAndSummaries hierarchy,
            Set<Identified> alreadyWritten
    ) {
        Series seriesSummary = hierarchy.getSeriesSummary().get();

        if (!alreadyWritten.contains(seriesSummary)) {
            writer.createOrUpdate(seriesSummary);
            alreadyWritten.add(seriesSummary);
        }
    }
}

package org.atlasapi.equiv.update;

import java.util.List;
import java.util.Set;

import org.atlasapi.equiv.update.metadata.EquivalenceUpdaterMetadata;
import org.atlasapi.equiv.update.metadata.RootEquivalenceUpdaterMetadata;
import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Series;
import org.atlasapi.media.entity.SeriesRef;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.reporting.telescope.OwlTelescopeProxy;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.atlasapi.media.entity.ChildRef.TO_URI;

public class RootEquivalenceUpdater implements EquivalenceUpdater<Content> {
    
    private static final Logger log = LoggerFactory.getLogger(RootEquivalenceUpdater.class);

    private ContentResolver contentResolver;
    private EquivalenceUpdater<Content> updater;

    private RootEquivalenceUpdater(
            ContentResolver contentResolver,
            EquivalenceUpdater<Content> updater
    ) {
        this.contentResolver = contentResolver;
        this.updater = updater;
    }

    public static RootEquivalenceUpdater create(
            ContentResolver contentResolver,
            EquivalenceUpdater<Content> updater
    ) {
        return new RootEquivalenceUpdater(contentResolver, updater);
    }

    @Override
    public boolean updateEquivalences(Content content, OwlTelescopeProxy telescopeProxy) {
        if (content instanceof Container) {
            return updateContainer((Container) content, telescopeProxy);
        } else if (content instanceof Item){
            return updateContentEquivalence(content, telescopeProxy);
        }
        return false;
    }

    @Override
    public EquivalenceUpdaterMetadata getMetadata(Set<Publisher> sources) {
        return RootEquivalenceUpdaterMetadata.create(
                updater.getMetadata(sources)
        );
    }

    private boolean updateContentEquivalence(Content content, OwlTelescopeProxy telescopeProxy) {
        log.trace("equiv update {}", content);
        return updater.updateEquivalences(content, telescopeProxy);
    }

    private boolean updateContainer(Container container, OwlTelescopeProxy telescopeProxy) {
        updateContentEquivalence(container, telescopeProxy);
        for (Item child : childrenOf(container)) {
            updateContentEquivalence(child, telescopeProxy);
        }
        if (container instanceof Brand) {
            for (Series series : seriesOf((Brand) container)) {
               updateContentEquivalence(series, telescopeProxy);
            }
        }
        return updateContentEquivalence(container, telescopeProxy);
    }

    private Iterable<Series> seriesOf(Brand brand) {
        ImmutableList<SeriesRef> seriesRefs = brand.getSeriesRefs();
        if (seriesRefs.isEmpty()) {
            return ImmutableList.of();
        }
        List<String> childUris = Lists.transform(seriesRefs, SeriesRef.TO_URI);
        ResolvedContent children = contentResolver.findByCanonicalUris(childUris);
        return Iterables.filter(children.getAllResolvedResults(), Series.class);
    }

    private Iterable<Item> childrenOf(Container content) {
        List<String> childUris = Lists.transform(content.getChildRefs(), TO_URI);
        ResolvedContent children = contentResolver.findByCanonicalUris(childUris);
        return Iterables.filter(children.getAllResolvedResults(), Item.class);
    }
}

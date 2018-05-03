package org.atlasapi.equiv.update;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.base.Maybe;
import org.atlasapi.equiv.handlers.ContainerSummaryRequiredException;
import org.atlasapi.equiv.update.metadata.EquivalenceUpdaterMetadata;
import org.atlasapi.equiv.update.metadata.RootEquivalenceUpdaterMetadata;
import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Series;
import org.atlasapi.media.entity.SeriesRef;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.reporting.telescope.OwlTelescopeReporter;

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
    public boolean updateEquivalences(Content content, OwlTelescopeReporter telescope) {
        if (content instanceof Container) {
            return updateContainer((Container) content, telescope);
        } else if (content instanceof Item){
            return updateContentEquivalence(content, telescope);
        }
        return false;
    }

    @Override
    public EquivalenceUpdaterMetadata getMetadata(Set<Publisher> sources) {
        return RootEquivalenceUpdaterMetadata.create(
                updater.getMetadata(sources)
        );
    }

    private boolean updateContentEquivalence(Content content, OwlTelescopeReporter telescope) {
        log.trace("equiv update {}", content);
        try {
            return updater.updateEquivalences(content, telescope);
        } catch (ContainerSummaryRequiredException e) {
            Optional<Identified> maybeContainer = contentResolver.findByCanonicalUris(ImmutableSet.of(e.getItem().getContainer().getUri()))
                    .getFirstValue()
                    .toOptional();

            if (maybeContainer.isPresent()) {
                // Try rerunning container equiv
                log.warn(
                        "Trying to rerun container {} equiv for item {}",
                        e.getItem().getContainer().getId(),
                        content.getId()
                );
                updater.updateEquivalences((Container) maybeContainer.get(), telescope);
                // Retry the failed content
                return updater.updateEquivalences(content, telescope);
            } else {
                log.error("Container {} not found", e.getItem().getContainer().getId(), e);
                return false;
            }
        }
    }

    private boolean updateContainer(Container container, OwlTelescopeReporter telescope) {
//        updateContentEquivalence(container, telescope);
//        for (Item child : childrenOf(container)) {
//            updateContentEquivalence(child, telescope);
//        }
        if (container instanceof Brand) {
            for (Series series : seriesOf((Brand) container)) {
               updateContentEquivalence(series, telescope);
            }
        }
        return updateContentEquivalence(container, telescope);
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

package org.atlasapi.equiv.update;

import java.util.Optional;
import java.util.Set;

import org.atlasapi.equiv.handlers.ContainerSummaryRequiredException;
import org.atlasapi.equiv.update.metadata.EquivalenceUpdaterMetadata;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.reporting.telescope.OwlTelescopeReporter;

import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This equivalence updater will try to equiv the container, if the child requires it.
 */
public class RecoveringEquivalenceUpdater implements EquivalenceUpdater<Content> {

    private static final Logger log = LoggerFactory.getLogger(RecoveringEquivalenceUpdater.class);

    private ContentResolver contentResolver;
    private EquivalenceUpdater<Content> updater;

    private RecoveringEquivalenceUpdater(
            ContentResolver contentResolver,
            EquivalenceUpdater<Content> updater
    ) {
        this.contentResolver = contentResolver;
        this.updater = updater;
    }

    public static RecoveringEquivalenceUpdater create(
            ContentResolver contentResolver,
            EquivalenceUpdater<Content> updater) {

        return new RecoveringEquivalenceUpdater(contentResolver, updater);
    }

    @Override
    public boolean updateEquivalences(Content content, OwlTelescopeReporter telescope) {
        try {
            return updater.updateEquivalences(content, telescope);
        } catch (ContainerSummaryRequiredException e) {
            try {
                Optional<Identified> container = contentResolver.findByCanonicalUris(
                        ImmutableSet.of(e.getItem().getContainer().getUri()))
                        .getFirstValue()
                        .toOptional();

                if (container.isPresent()) {
                    log.warn("Rerunning container {} equiv for item {}",
                            e.getItem().getContainer().getId(),
                            content.getId()
                    );
                    updater.updateEquivalences((Container) container.get(), telescope);
                    // Retry the failed content
                    return updater.updateEquivalences(content, telescope);
                } else {
                    log.error("Container {} not found", e.getItem().getContainer().getId(), e);
                    return false;
                }
            } catch (NullPointerException npe){ //because of e.getItem().getContainer().getUri()
                return false;
            }
        }
    }

    @Override
    public EquivalenceUpdaterMetadata getMetadata(Set<Publisher> sources) {
        return updater.getMetadata(sources);
    }
}

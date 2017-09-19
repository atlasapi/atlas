package org.atlasapi.equiv;

import static com.google.common.base.Preconditions.checkNotNull;

import org.atlasapi.equiv.handlers.ContainerSummaryRequiredException;
import org.atlasapi.equiv.results.persistence.EquivalenceResultStore;
import org.atlasapi.equiv.update.EquivalenceUpdater;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.messaging.v3.EntityUpdatedMessage;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;
import org.atlasapi.reporting.telescope.OwlTelescopeReporter;
import org.atlasapi.reporting.telescope.OwlTelescopeReporterFactory;
import org.atlasapi.reporting.telescope.OwlTelescopeReporters;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import com.metabroadcast.columbus.telescope.api.Event;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.properties.Configurer;
import com.metabroadcast.common.queue.Worker;

public class EquivalenceUpdatingWorker implements Worker<EntityUpdatedMessage> {

    private final Logger log = LoggerFactory.getLogger(getClass());
    
    private final ContentResolver contentResolver;
    private final LookupEntryStore entryStore;
    private final EquivalenceResultStore resultStore;
    private final EquivalenceUpdater<Content> equivUpdater;
    private final Predicate<Content> filter;
    private DateTime timeSinceTelescopeRotated;
    
    private final SubstitutionTableNumberCodec idCodec
        = SubstitutionTableNumberCodec.lowerCaseOnly();
    private OwlTelescopeReporter telescope;

    public EquivalenceUpdatingWorker(ContentResolver contentResolver,
            LookupEntryStore entryStore,
            EquivalenceResultStore resultStore, 
            EquivalenceUpdater<Content> equivUpdater, 
            Predicate<Content> filter) {
        this.contentResolver = checkNotNull(contentResolver);
        this.entryStore = checkNotNull(entryStore);
        this.resultStore = checkNotNull(resultStore);
        this.equivUpdater = checkNotNull(equivUpdater);
        this.filter = checkNotNull(filter);
    }

    @Override
    public void process(EntityUpdatedMessage message) {
        rotateTelescope();

        String eid = message.getEntityId();
        Content content = resolveId(idCodec.decode(eid).longValue());
        if (content == null) {
            log.warn("{} resolved null/not Content for {} {} {}", 
                new Object[]{message.getMessageId(), 
                    message.getEntitySource(), message.getEntityType(), eid});
            return;
        }
        if (filter.apply(content)) {
            log.debug("{} updating equivalence: {} {} {}", 
                new Object[]{message.getMessageId(), 
                    message.getEntitySource(), message.getEntityType(), eid});
            try {
                equivUpdater.updateEquivalences(content, telescope);
            } catch (ContainerSummaryRequiredException containerSummaryError) {
                log.trace("Container summary required, attempting to run", containerSummaryError);
                try {
                    Maybe<Identified> maybeContainer = contentResolver.findByCanonicalUris(
                            ImmutableSet.of(containerSummaryError.getItem()
                                    .getContainer()
                                    .getUri())).getFirstValue();
                    if (maybeContainer.hasValue()) {
                        Identified container = maybeContainer.requireValue();
                        equivUpdater.updateEquivalences((Content) container, telescope);
                        equivUpdater.updateEquivalences(containerSummaryError.getItem(), telescope);
                    } else {
                        log.error("Container summary missing AND container not resolved. Unable to "
                                + "run equivalence on item with ID: " + content.getId());
                    }
                } catch (Exception e) {
                    log.error("Failed to rerun equivalence for missing container summary", e);
                }
            }
        } else {
            log.trace("{} skipping equiv update: {} {} {}", 
                new Object[]{message.getMessageId(), 
                    message.getEntitySource(), message.getEntityType(), eid});
        }
    }

    private Content resolveId(Long id) {
        Iterable<LookupEntry> entries = entryStore.entriesForIds(ImmutableSet.of(id));
        LookupEntry entry = Iterables.getOnlyElement(entries, null);
        return entry != null ? resolveUri(entry.uri())
                             : null;
    }

    private Content resolveUri(String uri) {
        ImmutableSet<String> contentUri = ImmutableSet.of(uri);
        ResolvedContent resolved = contentResolver.findByCanonicalUris(contentUri);
        Maybe<Identified> possibleContent = resolved.get(uri);
        return isContent(possibleContent) ? (Content) possibleContent.requireValue()
                                          : null;
    }

    private boolean isContent(Maybe<Identified> possibleContent) {
        return possibleContent.valueOrNull() instanceof Content;
    }

    private synchronized void rotateTelescope() {
        if (telescope == null) {
            telescope = OwlTelescopeReporterFactory.getInstance().getTelescopeReporter(
                    OwlTelescopeReporters.EQUIVALENCE_UPDATING_WORKER,
                    Event.Type.EQUIVALENCE
            );
            telescope.startReporting();
            timeSinceTelescopeRotated = DateTime.now();
        } else if (timeSinceTelescopeRotated.plusHours(1).isBeforeNow()) {
            telescope.endReporting();
            telescope = OwlTelescopeReporterFactory.getInstance().getTelescopeReporter(
                    OwlTelescopeReporters.EQUIVALENCE_UPDATING_WORKER,
                    Event.Type.EQUIVALENCE
            );
            telescope.startReporting();
            timeSinceTelescopeRotated = DateTime.now();
        }
    }
}

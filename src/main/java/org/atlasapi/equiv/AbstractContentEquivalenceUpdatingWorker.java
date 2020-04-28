package org.atlasapi.equiv;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.metabroadcast.columbus.telescope.api.Event;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import org.atlasapi.equiv.handlers.ContainerSummaryRequiredException;
import org.atlasapi.equiv.update.EquivalenceUpdater;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Identified;
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

import static com.google.common.base.Preconditions.checkNotNull;

public abstract class AbstractContentEquivalenceUpdatingWorker {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final ContentResolver contentResolver;
    private final LookupEntryStore entryStore;
    private final EquivalenceUpdater<Content> equivUpdater;
    private final Predicate<Content> filter;
    private DateTime timeSinceTelescopeRotated;

    private final SubstitutionTableNumberCodec idCodec
        = SubstitutionTableNumberCodec.lowerCaseOnly();
    private OwlTelescopeReporter telescope;

    protected AbstractContentEquivalenceUpdatingWorker(ContentResolver contentResolver,
                                                    LookupEntryStore entryStore,
                                                    EquivalenceUpdater<Content> equivUpdater,
                                                    Predicate<Content> filter) {
        this.contentResolver = checkNotNull(contentResolver);
        this.entryStore = checkNotNull(entryStore);
        this.equivUpdater = checkNotNull(equivUpdater);
        this.filter = checkNotNull(filter);
    }

    public void process(String messageId, String contentId) {
        process(messageId, idCodec.decode(contentId).longValue());
    }

    public void process(String messageId, long contentId) {
        rotateTelescope();

        Content content = resolveId(contentId);
        if (content == null) {
            log.warn("{} resolved null/not Content for {}",
                    messageId,
                    contentId
            );
            return;
        }
        if (filter.apply(content)) {
            log.debug("{} updating equivalence: {}",
                    messageId,
                    contentId
            );
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
            log.trace("{} skipping equiv update: {}",
                    messageId,
                    contentId
            );
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

    protected abstract OwlTelescopeReporters getReporterName();

    private synchronized void rotateTelescope() {
        if (telescope == null) {
            telescope = OwlTelescopeReporterFactory.getInstance().getTelescopeReporter(
                    getReporterName(),
                    Event.Type.EQUIVALENCE
            );
            telescope.startReporting();
            timeSinceTelescopeRotated = DateTime.now();
        } else if (timeSinceTelescopeRotated.plusHours(1).isBeforeNow()) {
            telescope.endReporting();
            telescope = OwlTelescopeReporterFactory.getInstance().getTelescopeReporter(
                    getReporterName(),
                    Event.Type.EQUIVALENCE
            );
            telescope.startReporting();
            timeSinceTelescopeRotated = DateTime.now();
        }
    }
}

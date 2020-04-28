package org.atlasapi.equiv;

import com.google.common.base.Predicate;
import com.metabroadcast.common.queue.Worker;
import org.atlasapi.equiv.update.EquivalenceUpdater;
import org.atlasapi.media.entity.Content;
import org.atlasapi.messaging.v3.EquivalenceChangeMessage;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;
import org.atlasapi.reporting.telescope.OwlTelescopeReporters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EquivalenceChangesEquivalenceUpdatingWorker
        extends AbstractContentEquivalenceUpdatingWorker implements Worker<EquivalenceChangeMessage> {

    private final Logger log = LoggerFactory.getLogger(getClass());

    public EquivalenceChangesEquivalenceUpdatingWorker(
            ContentResolver contentResolver,
            LookupEntryStore entryStore,
            EquivalenceUpdater<Content> equivUpdater,
            Predicate<Content> filter
    ) {
        super(
                contentResolver,
                entryStore,
                equivUpdater,
                filter
        );
    }

    @Override
    public void process(EquivalenceChangeMessage message) {
        log.info(
                "Processing equiv change for subject: {} with outgoing changed ids: {}",
                message.getSubjectId(),
                message.getOutgoingIdsChanged()
        );
        for (long id : message.getOutgoingIdsChanged()) {
            if (id == message.getSubjectId()) {
                // The subject should always have a bidirectional link to itself.
                // We'll avoid running equiv on it again just in case it repeatedly causes messages to be produced.
                // This is added just as a precaution and has not been observed to happen.
                log.warn("Subject {} was listed as a changed outgoing id", message.getSubjectId());
                continue;
            }
            super.process(message.getMessageId(), id);
        }
    }

    @Override
    protected OwlTelescopeReporters getReporterName() {
        return OwlTelescopeReporters.EQUIVALENCE_CHANGES_EQUIVALENCE_UPDATING_WORKER;
    }
}

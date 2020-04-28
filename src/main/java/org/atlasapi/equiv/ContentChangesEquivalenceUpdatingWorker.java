package org.atlasapi.equiv;

import com.google.common.base.Predicate;
import com.metabroadcast.common.queue.Worker;
import org.atlasapi.equiv.update.EquivalenceUpdater;
import org.atlasapi.media.entity.Content;
import org.atlasapi.messaging.v3.EntityUpdatedMessage;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;
import org.atlasapi.reporting.telescope.OwlTelescopeReporters;

public class ContentChangesEquivalenceUpdatingWorker
        extends AbstractContentEquivalenceUpdatingWorker implements Worker<EntityUpdatedMessage> {

    public ContentChangesEquivalenceUpdatingWorker(ContentResolver contentResolver,
                                                   LookupEntryStore entryStore,
                                                   EquivalenceUpdater<Content> equivUpdater,
                                                   Predicate<Content> filter) {
        super(
                contentResolver,
                entryStore,
                equivUpdater,
                filter
        );
    }

    @Override
    public void process(EntityUpdatedMessage message) {
        super.process(message.getMessageId(), message.getEntityId());
    }

    @Override
    protected OwlTelescopeReporters getReporterName() {
        return OwlTelescopeReporters.CONTENT_CHANGES_EQUIVALENCE_UPDATING_WORKER;
    }
}

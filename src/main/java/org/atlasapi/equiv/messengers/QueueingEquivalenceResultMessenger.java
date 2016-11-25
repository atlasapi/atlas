package org.atlasapi.equiv.messengers;

import org.atlasapi.equiv.results.EquivalenceResult;
import org.atlasapi.equiv.results.scores.ScoredCandidate;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.messaging.ContentEquivalenceAssertionMessenger;
import org.atlasapi.messaging.v3.ContentEquivalenceAssertionMessage;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;

import com.metabroadcast.common.queue.MessageSender;
import com.metabroadcast.common.stream.MoreCollectors;
import com.metabroadcast.common.time.SystemClock;
import com.metabroadcast.common.time.Timestamper;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;

public class QueueingEquivalenceResultMessenger<T extends Content>
        implements EquivalenceResultMessenger<T> {

    private final ImmutableSet<Publisher> sources;
    private final ContentEquivalenceAssertionMessenger messenger;

    private QueueingEquivalenceResultMessenger(
            MessageSender<ContentEquivalenceAssertionMessage> sender,
            Iterable<Publisher> sources,
            LookupEntryStore lookupEntryStore,
            Timestamper timestamper
    ) {
        this.sources = ImmutableSet.copyOf(sources);
        this.messenger = ContentEquivalenceAssertionMessenger.create(
                sender,
                timestamper,
                lookupEntryStore
        );
    }

    /**
     * Create a new {@link QueueingEquivalenceResultMessenger}.
     * <p>
     * This messenger asserts direct equivalence or lack thereof on all publishers. This means all
     * outgoing equivalence edges need to be provided on every update and that the absence of an
     * equivalence assertion to any publisher will break any existing connections to that publisher.
     */
    public static <T extends Content> QueueingEquivalenceResultMessenger<T> create(
            MessageSender<ContentEquivalenceAssertionMessage> sender,
            LookupEntryStore lookupEntryStore
    ) {
        return new QueueingEquivalenceResultMessenger<>(
                sender,
                Publisher.all(),
                lookupEntryStore,
                new SystemClock()
        );
    }

    @VisibleForTesting
    static <T extends Content> QueueingEquivalenceResultMessenger<T> create(
            MessageSender<ContentEquivalenceAssertionMessage> sender,
            Iterable<Publisher> sources,
            LookupEntryStore lookupEntryStore,
            Timestamper timestamper
    ) {
        return new QueueingEquivalenceResultMessenger<>(
                sender, sources, lookupEntryStore, timestamper
        );
    }

    @Override
    public void sendMessage(EquivalenceResult<T> result) {
        messenger.sendMessage(
                result.subject(),
                result.strongEquivalences()
                        .values()
                        .stream()
                        .map(ScoredCandidate::candidate)
                        .collect(MoreCollectors.toImmutableList()),
                sources.stream()
                        .map(Publisher::key)
                        .collect(MoreCollectors.toImmutableSet())
        );
    }
}

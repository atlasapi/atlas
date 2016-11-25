package org.atlasapi.equiv.messengers;

import org.atlasapi.equiv.results.EquivalenceResult;
import org.atlasapi.equiv.results.description.DefaultDescription;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.messaging.v3.ContentEquivalenceAssertionMessage;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;

import com.metabroadcast.common.queue.MessageSender;
import com.metabroadcast.common.time.Timestamp;
import com.metabroadcast.common.time.Timestamper;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Longs;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class QueueingEquivalenceResultMessengerTest {

    @Mock private MessageSender<ContentEquivalenceAssertionMessage> sender;
    @Mock private Timestamper timestamper;
    @Mock private LookupEntryStore lookupEntryStore;

    private QueueingEquivalenceResultMessenger<Item> resultHandler;

    private Item graphItemWithLowestId;
    private Item subject;

    @Before
    public void setUp() throws Exception {
        resultHandler = QueueingEquivalenceResultMessenger.create(
                sender, Publisher.all(), lookupEntryStore, timestamper
        );

        when(timestamper.timestamp()).thenReturn(Timestamp.of(DateTime.now()));

        graphItemWithLowestId = new Item("uriA", "curieA", Publisher.METABROADCAST);
        graphItemWithLowestId.setId(0L);

        subject = new Item("uriB", "curieB", Publisher.BBC);
        subject.setId(10L);
    }

    @Test
    public void sendingMessageUsesGraphIdAsPartitionKey() throws Exception {
        EquivalenceResult<Item> result = new EquivalenceResult<>(
                subject,
                ImmutableList.of(),
                DefaultScoredCandidates.<Item>fromSource("src").build(),
                ImmutableMultimap.of(),
                new DefaultDescription()
        );

        when(lookupEntryStore.entriesForIds(ImmutableSet.of(subject.getId())))
                .thenReturn(ImmutableList.of(
                        LookupEntry.lookupEntryFrom(graphItemWithLowestId)
                                .copyWithEquivalents(ImmutableList.of(
                                        LookupRef.from(subject)
                                ))
                ));

        resultHandler.sendMessage(result);

        verify(sender).sendMessage(
                any(ContentEquivalenceAssertionMessage.class),
                eq(Longs.toByteArray(graphItemWithLowestId.getId()))
        );
    }

    @Test
    public void sendingMessageDefaultsToUsingSubjectIdAsPartitionKeyIfThereIsNoGraph()
            throws Exception {
        EquivalenceResult<Item> result = new EquivalenceResult<>(
                subject,
                ImmutableList.of(),
                DefaultScoredCandidates.<Item>fromSource("src").build(),
                ImmutableMultimap.of(),
                new DefaultDescription()
        );

        when(lookupEntryStore.entriesForIds(ImmutableSet.of(subject.getId())))
                .thenReturn(ImmutableList.of());

        resultHandler.sendMessage(result);

        verify(sender).sendMessage(
                any(ContentEquivalenceAssertionMessage.class),
                eq(Longs.toByteArray(subject.getId()))
        );
    }
}

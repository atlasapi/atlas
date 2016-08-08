package org.atlasapi.equiv.handlers;

import java.math.BigInteger;
import java.util.List;
import java.util.UUID;
import java.util.stream.StreamSupport;

import org.atlasapi.equiv.results.EquivalenceResult;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.messaging.v3.ContentEquivalenceAssertionMessage;
import org.atlasapi.messaging.v3.ContentEquivalenceAssertionMessage.AdjacentRef;

import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.queue.MessageSender;
import com.metabroadcast.common.stream.MoreCollectors;
import com.metabroadcast.common.time.SystemClock;
import com.metabroadcast.common.time.Timestamp;
import com.metabroadcast.common.time.Timestamper;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class MessageQueueingResultHandler<T extends Content>
        implements EquivalenceResultHandler<T> {

    private static final Logger log = LoggerFactory.getLogger(MessageQueueingResultHandler.class);
    
    private final MessageSender<ContentEquivalenceAssertionMessage> sender;
    private final Timestamper stamper;
    private final ImmutableSet<String> sourceKeys;
    
    private final NumberToShortStringCodec entityIdCodec;

    public MessageQueueingResultHandler(
            MessageSender<ContentEquivalenceAssertionMessage> sender,
            Iterable<Publisher> sources
    ) {
        this(sender, sources, new SystemClock());
    }

    @VisibleForTesting
    MessageQueueingResultHandler(
            MessageSender<ContentEquivalenceAssertionMessage> sender,
            Iterable<Publisher> sources,
            Timestamper stamper
    ) {
        this.sender = checkNotNull(sender);
        this.stamper = checkNotNull(stamper);
        this.sourceKeys = StreamSupport.stream(sources.spliterator(), false)
                .map(Publisher::key)
                .collect(MoreCollectors.toImmutableSet());

        this.entityIdCodec = SubstitutionTableNumberCodec.lowerCaseOnly();
    }
    
    @Override
    public void handle(EquivalenceResult<T> result) {
        try {
            ContentEquivalenceAssertionMessage message = messageFrom(result);
            for (AdjacentRef adjacentRef : message.getAdjacent()) {
                log.trace(
                        "Subject: {} Adjacent: {}",
                        result.subject().getCanonicalUri(),
                        adjacentRef.toString()
                );
            }
            sender.sendMessage(message, message.getEntityId().getBytes());
        } catch (Exception e) {
            log.error("Failed to send equiv update message: " + result.subject(), e);
        }
    }

    private ContentEquivalenceAssertionMessage messageFrom(EquivalenceResult<T> result) {
        String messageId = UUID.randomUUID().toString();
        Timestamp timestamp = stamper.timestamp();
        T subject = result.subject();
        String subjectId = entityIdCodec.encode(BigInteger.valueOf(subject.getId()));
        String subjectType = subject.getClass().getSimpleName().toLowerCase();
        String subjectSource = subject.getPublisher().key();

        return new ContentEquivalenceAssertionMessage(
                messageId,
                timestamp,
                subjectId,
                subjectType,
                subjectSource,
                adjacents(result),
                sourceKeys
        );
    }

    private List<AdjacentRef> adjacents(EquivalenceResult<T> result) {
        return Lists.newArrayList(Collections2.transform(result.strongEquivalences().values(),
                input -> {
                    @SuppressWarnings("ConstantConditions")
                    T candidate = input.candidate();

                    return new AdjacentRef(
                        entityIdCodec.encode(BigInteger.valueOf(candidate.getId())),
                        candidate.getClass().getSimpleName().toLowerCase(),
                        candidate.getPublisher().key()
                    );
                }
        ));
    }
}

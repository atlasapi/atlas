package org.atlasapi.equiv.handlers;

import static com.google.common.base.Preconditions.checkNotNull;

import java.math.BigInteger;
import java.util.List;
import java.util.UUID;

import org.atlasapi.equiv.results.EquivalenceResult;
import org.atlasapi.equiv.results.scores.ScoredCandidate;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.messaging.v3.ContentEquivalenceAssertionMessage;
import org.atlasapi.messaging.v3.ContentEquivalenceAssertionMessage.AdjacentRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.queue.MessageSender;
import com.metabroadcast.common.time.SystemClock;
import com.metabroadcast.common.time.Timestamp;
import com.metabroadcast.common.time.Timestamper;

public class MessageQueueingResultHandler<T extends Content>
        implements EquivalenceResultHandler<T> {

    private final Logger log = LoggerFactory.getLogger(getClass());
    
    private final MessageSender<ContentEquivalenceAssertionMessage> sender;
    private final Timestamper stamper;
    private final ImmutableSet<String> sourceKeys;
    
    private final NumberToShortStringCodec entityIdCodec = SubstitutionTableNumberCodec.lowerCaseOnly();

    public MessageQueueingResultHandler(MessageSender<ContentEquivalenceAssertionMessage> sender, Iterable<Publisher> sources) {
        this(sender, sources, new SystemClock());
    }
    
    public MessageQueueingResultHandler(MessageSender<ContentEquivalenceAssertionMessage> sender, Iterable<Publisher> sources, 
            Timestamper stamper) {
        this.sender = checkNotNull(sender);
        this.stamper = checkNotNull(stamper);
        this.sourceKeys = ImmutableSet.copyOf(Iterables.transform(sources,Publisher.TO_KEY));
    }
    
    @Override
    public void handle(EquivalenceResult<T> result) {
        try {
            ContentEquivalenceAssertionMessage message = messageFrom(result);
            for (AdjacentRef adjacentRef : message.getAdjacent()) {
                log.trace("Subject: {} Adjacent: {}", result.subject().getCanonicalUri(), adjacentRef.toString());
            }
            sender.sendMessage(message, message.getEntityId().getBytes());
        } catch (Exception e) {
            log.error("Failed to send equiv update message: " + result.subject(), e);
        }
    }

    private ContentEquivalenceAssertionMessage messageFrom(EquivalenceResult<T> result) {
        String mid = UUID.randomUUID().toString();
        Timestamp ts = stamper.timestamp();
        T subject = result.subject();
        String subjectId = entityIdCodec.encode(BigInteger.valueOf(subject.getId()));
        String subjectType = subject.getClass().getSimpleName().toLowerCase();
        String subjectSource = subject.getPublisher().key();
        return new ContentEquivalenceAssertionMessage(mid, ts, 
                subjectId, subjectType, subjectSource, 
                adjacents(result), sourceKeys);
    }

    private List<AdjacentRef> adjacents(EquivalenceResult<T> result) {
        return Lists.newArrayList(Collections2.transform(result.strongEquivalences().values(),
            new Function<ScoredCandidate<T>, AdjacentRef>() {
                @Override
                public AdjacentRef apply(ScoredCandidate<T> input) {
                    T cand = input.candidate();
                    return new AdjacentRef(
                        entityIdCodec.encode(BigInteger.valueOf(cand.getId())),
                        cand.getClass().getSimpleName().toLowerCase(),
                        cand.getPublisher().key()
                    );
                }
            }
        ));
    }
    
}

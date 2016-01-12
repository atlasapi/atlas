package org.atlasapi.equiv.handlers;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.atlasapi.equiv.results.EquivalenceResult;
import org.atlasapi.equiv.results.description.DefaultDescription;
import org.atlasapi.equiv.results.description.ReadableDescription;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidate;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.messaging.v3.ContentEquivalenceAssertionMessage;
import org.atlasapi.messaging.v3.ContentEquivalenceAssertionMessage.AdjacentRef;
import org.atlasapi.messaging.v3.JacksonMessageSerializer;
import org.atlasapi.messaging.v3.KafkaMessagingModule;
import org.atlasapi.messaging.v3.MessagingModule;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.queue.MessageSender;
import com.metabroadcast.common.queue.MessageSerializer;
import com.metabroadcast.common.queue.Worker;
import com.metabroadcast.common.queue.kafka.KafkaConsumer;
import com.metabroadcast.common.queue.kafka.KafkaTestBase;

import kafka.admin.AdminUtils;
import kafka.utils.TestUtils;
import kafka.zk.EmbeddedZookeeper;
import scala.Option;
import scala.collection.JavaConversions;

public class MessageQueueingResultHandlerTest extends KafkaTestBase {

    private MessageQueueingResultHandler<Item> handler;
    private MessagingModule mm;
    private MessageSerializer<ContentEquivalenceAssertionMessage> serializer;
    
    private String topic = "EquivAssert";
    private String system = "AtlasOwlTest";
    private String namespacedTopic = system + topic;

    @Before
    public void setup() throws Exception {
        Logger.getRootLogger().setLevel(Level.FATAL);
        Logger.getLogger("com.metabroadcast").setLevel(Level.TRACE);
        super.setup();
        EmbeddedZookeeper zkServer = super.zkServer();
        mm = new KafkaMessagingModule(super.brokersString(), zkServer.connectString(), system, 100L, 1000L);
        serializer = JacksonMessageSerializer.forType(ContentEquivalenceAssertionMessage.class);
        
        MessageSender<ContentEquivalenceAssertionMessage> sender
            = mm.messageSenderFactory().makeMessageSender(topic, serializer);
        handler = new MessageQueueingResultHandler<Item>(sender, Publisher.all());
    }

    @Test
    public void testHandleSendsAMessage() throws Exception {
        
        Item subject = new Item("s","s",Publisher.BBC);
        subject.setId(1225L);
        Item equivalent = new Item("e","e",Publisher.PA);
        equivalent.setId(830L);
        List<ScoredCandidates<Item>> scores = ImmutableList.of();
        ScoredCandidates<Item> combined = DefaultScoredCandidates.<Item>fromSource("src").build();
        Map<Publisher, ScoredCandidate<Item>> strong = ImmutableMap.of(
            Publisher.PA, ScoredCandidate.valueOf(equivalent, Score.ONE));
        ReadableDescription desc = new DefaultDescription();
        
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<ContentEquivalenceAssertionMessage> message
            = new AtomicReference<ContentEquivalenceAssertionMessage>();
        
        Worker<ContentEquivalenceAssertionMessage> worker
            = new Worker<ContentEquivalenceAssertionMessage>() {
                @Override
                public void process(ContentEquivalenceAssertionMessage equivalenceAssertionMessage) {
                    message.set(equivalenceAssertionMessage);
                    latch.countDown();
                }
            };
            
        AdminUtils.createTopic(zkClient(), namespacedTopic, 1, 2, new Properties());
        TestUtils.waitUntilMetadataIsPropagated(JavaConversions.asScalaBuffer(kafkaServers()), namespacedTopic, 0, 5000);
        TestUtils.waitUntilLeaderIsElectedOrChanged(zkClient(), namespacedTopic, 0, 500, Option.empty());
        
        KafkaConsumer consumer = (KafkaConsumer) mm.messageConsumerFactory().createConsumer(worker, serializer, topic, "Deserializer")
                .withDefaultConsumers(2)
                .build();
        consumer.startAsync().awaitRunning(10, TimeUnit.SECONDS);

        handler.handle(new EquivalenceResult<>(subject, scores, combined, strong, desc));
        assertTrue("message not received", latch.await(5, TimeUnit.SECONDS));
        
        ContentEquivalenceAssertionMessage assertionMessage = message.get();
        
        assertThat(assertionMessage.getEntityId(), is("cyp"));
        assertThat(assertionMessage.getEntityType(), is(subject.getClass().getSimpleName().toLowerCase()));
        assertThat(assertionMessage.getEntitySource(), is(subject.getPublisher().key()));

        AdjacentRef adjRef = Iterables.getOnlyElement(assertionMessage.getAdjacent());
        assertThat(adjRef.getId(), is("cf2"));
        assertThat(adjRef.getSource(), is(equivalent.getPublisher().key()));
        assertThat(adjRef.getType(), is(equivalent.getClass().getSimpleName().toLowerCase()));
        
        assertThat(ImmutableSet.copyOf(assertionMessage.getSources()), 
                is(ImmutableSet.copyOf(Iterables.transform(Publisher.all(),Publisher.TO_KEY))));
        
    }

}

package org.atlasapi.equiv.handlers;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

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
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;

import com.metabroadcast.common.queue.MessageSender;
import com.metabroadcast.common.queue.MessageSerializer;
import com.metabroadcast.common.queue.Worker;
import com.metabroadcast.common.queue.kafka.KafkaConsumer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import kafka.admin.AdminUtils;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.SystemTime$;
import kafka.utils.TestUtils;
import kafka.utils.ZKStringSerializer$;
import kafka.zk.EmbeddedZookeeper;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import scala.Option;
import scala.collection.JavaConversions;
import scala.collection.mutable.Buffer;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyCollectionOf;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class MessageQueueingResultHandlerIT {

    private static String topic = "EquivAssert";
    private static String system = "AtlasOwlTest";
    private static String namespacedTopic = system + topic;

    private static EmbeddedZookeeper zkServer;
    private static ZkClient zkClient;
    private static List<KafkaServer> kafkaServers;

    private MessageQueueingResultHandler<Item> handler;
    private MessagingModule mm;
    private MessageSerializer<ContentEquivalenceAssertionMessage> serializer;

    @Mock private LookupEntryStore lookupEntryStore;

    @BeforeClass
    public static void init() throws Exception {
        String zkConnect = "127.0.0.1:" + TestUtils.choosePort();
        zkServer = new EmbeddedZookeeper(zkConnect);
        zkClient = createZkClient(zkConnect);

        ImmutableList.Builder builder = ImmutableList.builder();
        int port1 = TestUtils.choosePort();
        Properties brokerConfig1 = TestUtils.createBrokerConfig(0, port1);
        brokerConfig1.put("zookeeper.connect", zkConnect);
        KafkaConfig config1 = new KafkaConfig(brokerConfig1);
        builder.add(TestUtils.createServer(config1, SystemTime$.MODULE$));

        int port2 = TestUtils.choosePort();
        Properties brokerConfig2 = TestUtils.createBrokerConfig(1, port2);
        brokerConfig2.put("zookeeper.connect", zkConnect);
        KafkaConfig config2 = new KafkaConfig(brokerConfig2);
        config2.props().props().put("zookeeper.connect", zkConnect);
        builder.add(TestUtils.createServer(config2, SystemTime$.MODULE$));

        kafkaServers = builder.build();

        AdminUtils.createTopic(
                zkClient, namespacedTopic, 2, 2, new Properties()
        );
        TestUtils.waitUntilMetadataIsPropagated(
                JavaConversions.asScalaBuffer(kafkaServers), namespacedTopic, 0, 1000
        );
        TestUtils.waitUntilLeaderIsElectedOrChanged(
                zkClient, namespacedTopic, 0, 500, Option.empty()
        );
    }

    @Before
    public void setup() throws Exception {
        mm = new KafkaMessagingModule(
                brokersString(),
                zkServer.connectString(),
                system,
                100L,
                1000L
        );
        serializer = JacksonMessageSerializer.forType(ContentEquivalenceAssertionMessage.class);
        
        MessageSender<ContentEquivalenceAssertionMessage> sender
            = mm.messageSenderFactory().makeMessageSender(topic, serializer);
        handler = MessageQueueingResultHandler.create(sender, Publisher.all(), lookupEntryStore);
    }

    @Test
    public void testHandleSendsAMessage() throws Exception {
        when(lookupEntryStore.entriesForIds(anyCollectionOf(Long.class)))
                .thenReturn(ImmutableSet.of());
        
        Item subject = new Item("s","s",Publisher.BBC);
        subject.setId(1225L);
        Item equivalent = new Item("e","e",Publisher.PA);
        equivalent.setId(830L);
        List<ScoredCandidates<Item>> scores = ImmutableList.of();
        ScoredCandidates<Item> combined = DefaultScoredCandidates.<Item>fromSource("src").build();
        Multimap<Publisher, ScoredCandidate<Item>> strong = ImmutableMultimap.of(
            Publisher.PA, ScoredCandidate.valueOf(equivalent, Score.ONE));
        ReadableDescription desc = new DefaultDescription();
        
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<ContentEquivalenceAssertionMessage> message
            = new AtomicReference<ContentEquivalenceAssertionMessage>();
        
        Worker<ContentEquivalenceAssertionMessage> worker = equivalenceAssertionMessage -> {
            message.set(equivalenceAssertionMessage);
            latch.countDown();
        };
            
        KafkaConsumer consumer = (KafkaConsumer) mm.messageConsumerFactory()
                .createConsumer(worker, serializer, topic, "Deserializer")
                .withDefaultConsumers(2)
                .build();
        consumer.startAsync().awaitRunning(10, TimeUnit.SECONDS);

        handler.handle(new EquivalenceResult<>(subject, scores, combined, strong, desc));
        assertTrue("message not received", latch.await(5, TimeUnit.SECONDS));
        
        ContentEquivalenceAssertionMessage assertionMessage = message.get();
        
        assertThat(assertionMessage.getEntityId(), is("cyp"));
        assertThat(assertionMessage.getEntityType(),
                is(subject.getClass().getSimpleName().toLowerCase()));
        assertThat(assertionMessage.getEntitySource(), is(subject.getPublisher().key()));

        AdjacentRef adjRef = Iterables.getOnlyElement(assertionMessage.getAdjacent());
        assertThat(adjRef.getId(), is("cf2"));
        assertThat(adjRef.getSource(), is(equivalent.getPublisher().key()));
        assertThat(adjRef.getType(), is(equivalent.getClass().getSimpleName().toLowerCase()));
        
        assertThat(ImmutableSet.copyOf(assertionMessage.getSources()), 
                is(ImmutableSet.copyOf(Iterables.transform(Publisher.all(),Publisher.TO_KEY))));
    }

    @AfterClass
    public static void cleanUp() throws Exception {
        for (KafkaServer kafkaServer : kafkaServers) {
            kafkaServer.shutdown();
            kafkaServer.awaitShutdown();
            Thread.sleep(1000);
        }
        zkClient.close();
        zkServer.shutdown();
    }

    private static ZkClient createZkClient(String zkAddr) {
        int sessTimeout = 6000;
        int connTimeout = 6000;
        ZkSerializer zkStringSerializer = ZKStringSerializer$.MODULE$;
        ZkClient zkClient = new ZkClient(zkAddr, sessTimeout, connTimeout, zkStringSerializer);
        zkClient.waitUntilConnected();
        return zkClient;
    }

    private String brokersString() {
        Buffer<KafkaConfig> cfgs = JavaConversions.asScalaBuffer(
                Lists.transform(kafkaServers, KafkaServer::config)
        );
        return TestUtils.getBrokerListStrFromConfigs(cfgs);
    }
}

package org.atlasapi.remotesite.pa;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.io.Resources;
import com.google.common.util.concurrent.MoreExecutors;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.media.MimeType;
import com.metabroadcast.common.persistence.MongoTestHelper;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.persistence.mongo.DatabasedMongoClient;
import com.metabroadcast.common.queue.MessageSender;
import com.metabroadcast.common.queue.MessagingException;
import com.metabroadcast.common.time.DateTimeZones;
import com.metabroadcast.common.time.TimeMachine;
import com.mongodb.ReadPreference;
import junit.framework.TestCase;
import org.apache.commons.io.FileUtils;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelQuery;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Image;
import org.atlasapi.media.entity.ImageType;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.MediaType;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Specialization;
import org.atlasapi.media.entity.Version;
import org.atlasapi.messaging.v3.ScheduleUpdateMessage;
import org.atlasapi.persistence.audit.NoLoggingPersistenceAuditLog;
import org.atlasapi.persistence.audit.PersistenceAuditLog;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.EquivalentContentResolver;
import org.atlasapi.persistence.content.LookupResolvingContentResolver;
import org.atlasapi.persistence.content.mongo.MongoContentResolver;
import org.atlasapi.persistence.content.mongo.MongoContentWriter;
import org.atlasapi.persistence.content.people.DummyItemsPeopleWriter;
import org.atlasapi.persistence.content.schedule.mongo.MongoScheduleStore;
import org.atlasapi.persistence.logging.AdapterLog;
import org.atlasapi.persistence.logging.SystemOutAdapterLog;
import org.atlasapi.persistence.lookup.mongo.MongoLookupEntryStore;
import org.atlasapi.persistence.player.PlayerResolver;
import org.atlasapi.persistence.service.ServiceResolver;
import org.atlasapi.remotesite.channel4.pmlsd.epg.BroadcastTrimmer;
import org.atlasapi.remotesite.pa.data.DefaultPaProgrammeDataStore;
import org.atlasapi.remotesite.pa.deletes.ExistingItemUnPublisher;
import org.atlasapi.remotesite.pa.persistence.PaScheduleVersionStore;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.integration.junit4.JMock;
import org.joda.time.DateTime;
import org.joda.time.DateTimeConstants;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.joda.time.LocalDate;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import javax.annotation.Nullable;
import java.io.File;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;

@RunWith(JMock.class)
public class PaBaseProgrammeUpdaterTest extends TestCase {

    private static final String TMP_TEST_DIRECTORY = "/tmp/atlas_test_data_pa";

    private Mockery context = new Mockery();

    private PaProgDataProcessor programmeProcessor;

    private TimeMachine clock = new TimeMachine();
    private AdapterLog log = new SystemOutAdapterLog();
    private ContentResolver resolver;
    private ContentWriter contentWriter;
    private MongoScheduleStore scheduleWriter;

    private final ServiceResolver serviceResolver = mock(ServiceResolver.class);
    private final PlayerResolver playerResolver = mock(PlayerResolver.class);
    private final PersistenceAuditLog persistenceAuditLog = new NoLoggingPersistenceAuditLog();

    private ChannelResolver channelResolver;
    private ContentBuffer contentBuffer;
    private MessageSender<ScheduleUpdateMessage> ms = new MessageSender<ScheduleUpdateMessage>() {

        @Override
        public void close() throws Exception {
        }

        @Override
        public void sendMessage(ScheduleUpdateMessage message) throws MessagingException {
        }

        @Override
        public void sendMessage(ScheduleUpdateMessage scheduleUpdateMessage, byte[] bytes)
                throws MessagingException {
        }
    };
    private final PaTagMap paTagMap = mock(PaTagMap.class);

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        DatabasedMongo db = MongoTestHelper.anEmptyTestDatabase();
        DatabasedMongoClient dbWithClient = MongoTestHelper.anEmptyTestDatabaseWithMongoClient();
        MongoLookupEntryStore lookupStore = new MongoLookupEntryStore(
                dbWithClient,
                "lookup",
                persistenceAuditLog,
                ReadPreference.primary()
        );
        resolver = new LookupResolvingContentResolver(
                new MongoContentResolver(db, lookupStore),
                lookupStore
        );

        channelResolver = new DummyChannelResolver();
        contentWriter = new MongoContentWriter(
                db,
                lookupStore,
                persistenceAuditLog,
                playerResolver,
                serviceResolver,
                clock
        );
        programmeProcessor = PaProgrammeProcessor.create(
                resolver,
                log,
                paTagMap,
                mock(ExistingItemUnPublisher.class)
        );
        EquivalentContentResolver equivContentResolver = context.mock(
                EquivalentContentResolver.class
        );
        scheduleWriter = new MongoScheduleStore(
                db,
                channelResolver,
                contentBuffer,
                contentWriter,
                equivContentResolver,
                ms
        );
        contentBuffer = ContentBuffer.create(resolver, contentWriter, new DummyItemsPeopleWriter());

        File tmpTestDirectory = new File(TMP_TEST_DIRECTORY);
        if (tmpTestDirectory.exists()) {
            FileUtils.forceDelete(tmpTestDirectory);
        }
        FileUtils.forceMkdir(tmpTestDirectory);
        FileUtils.forceDeleteOnExit(tmpTestDirectory);
    }

    @Test
    public void testShouldCreateCorrectPaData() throws Exception {
        PaScheduleVersionStore scheduleVersionStore = context.mock(
                PaScheduleVersionStore.class
        );
        BroadcastTrimmer broadcastTrimmer = context.mock(BroadcastTrimmer.class);

        Channel channel = channelResolver.fromUri("http://www.bbc.co.uk/bbcone")
                .requireValue();
        LocalDate scheduleDay = new LocalDate(2011, DateTimeConstants.JANUARY, 15);

        context.checking(new Expectations() {{
            ignoring(broadcastTrimmer);
            oneOf(scheduleVersionStore).get(channel, scheduleDay);
            will(returnValue(Optional.<Long>absent()));
            oneOf(scheduleVersionStore).store(channel, scheduleDay, 1);
            oneOf(scheduleVersionStore).get(channel, scheduleDay);
            will(returnValue(Optional.of(1L)));
            oneOf(scheduleVersionStore).store(channel, scheduleDay, 201202251115L);
            oneOf(scheduleVersionStore).get(channel, scheduleDay);
            will(returnValue(Optional.of(201202251115L)));
            oneOf(scheduleVersionStore).get(channel, scheduleDay);
            will(returnValue(Optional.of(201202251115L)));
        }});

        TestPaProgrammeUpdater updater = new TestPaProgrammeUpdater(
                channelResolver,
                ImmutableList.of(
                        new File(Resources.getResource("20110115_tvdata.xml").getFile()),
                        new File(Resources.getResource("201202251115_20110115_tvdata.xml")
                                .getFile())
                ),
                scheduleVersionStore,
                PaChannelProcessor.builder()
                        .withProcessor(programmeProcessor)
                        .withTrimmer(broadcastTrimmer)
                        .withScheduleWriter(scheduleWriter)
                        .withScheduleVersionStore(scheduleVersionStore)
                        .withContentBuffer(contentBuffer)
                        .build()
        );
        updater.run();

        Identified content;

        content = resolver.findByCanonicalUris(ImmutableList.of(
                "http://pressassociation.com/brands/122139")).get(
                "http://pressassociation.com/brands/122139").requireValue();

        assertNotNull(content);
        assertTrue(content instanceof Brand);
        Brand brand = (Brand) content;
        assertFalse(brand.getChildRefs().isEmpty());
        assertNotNull(brand.getImage());
        Image brandImage = Iterables.getOnlyElement(brand.getImages());
        assertEquals(
                "http://images.atlas.metabroadcast.com/pressassociation.com/webcomeflywithme1.jpg",
                brandImage.getCanonicalUri()
        );
        assertEquals(new DateTime(2010, DateTimeConstants.DECEMBER, 18, 0, 0, 0, 0).withZone(
                DateTimeZone.UTC), brandImage.getAvailabilityStart());
        assertEquals(new DateTime(2011, DateTimeConstants.FEBRUARY, 6, 0, 0, 0, 0).withZone(
                DateTimeZone.UTC), brandImage.getAvailabilityEnd());
        assertEquals(MimeType.IMAGE_JPG, brandImage.getMimeType());
        assertEquals(ImageType.PRIMARY, brandImage.getType());
        assertEquals((Integer) 1024, brandImage.getWidth());
        assertEquals((Integer) 576, brandImage.getHeight());

        Item item = loadItemAtPosition(brand, 0);
        assertTrue(item.getCanonicalUri().contains("episodes"));
        assertNotNull(item.getImage());
        assertFalse(item.getVersions().isEmpty());
        assertEquals(MediaType.VIDEO, item.getMediaType());
        assertEquals(Specialization.TV, item.getSpecialization());

        assertEquals(17, item.people().size());
        assertEquals(14, item.actors().size());

        Version version = item.getVersions().iterator().next();
        assertFalse(version.getBroadcasts().isEmpty());
        assertTrue(version.is3d());

        Iterator<Broadcast> broadcasts = version.getBroadcasts().iterator();
        Broadcast broadcast1 = broadcasts.next();
        assertEquals("pa:71118471", broadcast1.getSourceId());
        Broadcast broadcast2 = broadcasts.next();
        assertEquals("pa:71118472", broadcast2.getSourceId());

        updater.run();
        Thread.sleep(1000);

        content = resolver.findByCanonicalUris(ImmutableList.of(
                "http://pressassociation.com/brands/122139")).get(
                "http://pressassociation.com/brands/122139").requireValue();
        assertNotNull(content);
        assertTrue(content instanceof Brand);
        brand = (Brand) content;
        assertFalse(brand.getChildRefs().isEmpty());

        item = loadItemAtPosition(brand, 0);
        assertFalse(item.getVersions().isEmpty());

        version = item.getVersions().iterator().next();
        assertFalse(version.getBroadcasts().isEmpty());

        broadcasts = version.getBroadcasts().iterator();
        broadcast1 = broadcasts.next();
        assertEquals("pa:71118471", broadcast1.getSourceId());
        assertTrue(broadcast1.getRepeat());
    }

    @Test
    public void testBroadcastsTrimmerWindowNoTimesInFile() {
        PaScheduleVersionStore scheduleVersionStore = context.mock(
                PaScheduleVersionStore.class
        );
        BroadcastTrimmer trimmer = context.mock(BroadcastTrimmer.class);

        Interval firstFileInterval = new Interval(new DateTime(
                2011,
                DateTimeConstants.JANUARY,
                15,
                6,
                0,
                0,
                0,
                DateTimeZones.LONDON
        ), new DateTime(2011, DateTimeConstants.JANUARY, 16, 6, 0, 0, 0, DateTimeZones.LONDON));

        context.checking(new Expectations() {{
            ignoring(scheduleVersionStore);
            oneOf(trimmer).trimBroadcasts(
                    firstFileInterval,
                    channelResolver.fromUri("http://www.bbc.co.uk/bbcone").requireValue(),
                    ImmutableMap.of("pa:71118471", "http://pressassociation.com/episodes/1424497")
            );
        }});

        // The reason for setting a null schedule version store in the updater a non-null one in the
        // channel processor is that the former can deal with the lack of a version store while
        // the latter cannot and will NPE. This needs a rethink of the PA ingester to determine
        // whether it's legitimate to have them behave differently
        TestPaProgrammeUpdater updater = new TestPaProgrammeUpdater(
                channelResolver,
                ImmutableList.of(new File(Resources.getResource("20110115_tvdata.xml").getFile())),
                null,
                PaChannelProcessor.builder()
                        .withProcessor(programmeProcessor)
                        .withTrimmer(trimmer)
                        .withScheduleWriter(scheduleWriter)
                        .withScheduleVersionStore(scheduleVersionStore)
                        .withContentBuffer(contentBuffer)
                        .build()
        );
        updater.run();
    }

    @Test
    public void testBroadcastTrimmerWindowTimesInFile() {
        PaScheduleVersionStore scheduleVersionStore = context.mock(
                PaScheduleVersionStore.class
        );
        BroadcastTrimmer trimmer = context.mock(BroadcastTrimmer.class);

        Interval fileInterval = new Interval(new DateTime(
                2011,
                DateTimeConstants.JANUARY,
                15,
                21,
                40,
                0,
                0,
                DateTimeZones.LONDON
        ), new DateTime(2011, DateTimeConstants.JANUARY, 15, 23, 30, 0, 0, DateTimeZones.LONDON));

        context.checking(new Expectations() {{
            ignoring(scheduleVersionStore);
            oneOf(trimmer).trimBroadcasts(
                    fileInterval,
                    channelResolver.fromUri("http://www.bbc.co.uk/bbcone").requireValue(),
                    ImmutableMap.of("pa:71118472", "http://pressassociation.com/episodes/1424497")
            );
        }});

        // The reason for setting a null schedule version store in the updater a non-null one in the
        // channel processor is that the former can deal with the lack of a version store while
        // the latter cannot and will NPE. This needs a rethink of the PA ingester to determine
        // whether it's legitimate to have them behave differently
        TestPaProgrammeUpdater updater = new TestPaProgrammeUpdater(
                channelResolver,
                ImmutableList.of(new File(Resources.getResource("201202251115_20110115_tvdata.xml")
                        .getFile())),
                null,
                PaChannelProcessor.builder()
                        .withProcessor(programmeProcessor)
                        .withTrimmer(trimmer)
                        .withScheduleWriter(scheduleWriter)
                        .withScheduleVersionStore(scheduleVersionStore)
                        .withContentBuffer(contentBuffer)
                        .build()
        );
        updater.run();
    }

    private Item loadItemAtPosition(Brand brand, int index) {
        return (Item) resolver.findByCanonicalUris(ImmutableList.of(brand.getChildRefs()
                .get(index)
                .getUri())).getFirstValue().requireValue();
    }

    static class TestPaProgrammeUpdater extends PaBaseProgrammeUpdater {

        private List<File> files;

        public TestPaProgrammeUpdater(
                ChannelResolver channelResolver,
                List<File> files,
                @Nullable PaScheduleVersionStore scheduleVersionStore,
                PaChannelProcessor paChannelProcessor
        ) {
            super(
                    MoreExecutors.sameThreadExecutor(),
                    paChannelProcessor,
                    new DefaultPaProgrammeDataStore(TMP_TEST_DIRECTORY, null),
                    channelResolver,
                    Optional.fromNullable(scheduleVersionStore),
                    Mode.NORMAL
            );
            this.files = files;
        }

        @Override
        public void runTask() {
            this.processFiles(files);
        }
    }

    static class DummyChannelResolver implements ChannelResolver {

        private final Channel channel = Channel.builder()
                .withSource(Publisher.METABROADCAST)
                .withTitle("BBC One")
                .withKey("bbcone")
                .withHighDefinition(false)
                .withMediaType(MediaType.VIDEO)
                .withUri("http://www.bbc.co.uk/bbcone")
                .build();

        public DummyChannelResolver() {
            channel.setId(1L);
        }

        @Override
        public Maybe<Channel> fromKey(String key) {
            if ("bbcone".equals(key)) {
                return Maybe.just(channel);
            }
            return Maybe.just(Channel.builder().build());
        }

        @Override
        public Maybe<Channel> fromId(long id) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Maybe<Channel> fromUri(String uri) {
            if ("http://www.bbc.co.uk/bbcone".equals(uri)) {
                return Maybe.just(channel);
            }
            return Maybe.just(Channel.builder().build());
        }

        @Override
        public Iterable<Channel> all() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<String, Channel> forAliases(String aliasPrefix) {
            return ImmutableMap.of(
                    "http://pressassociation.com/channels/4", channel);
        }

        @Override
        public Iterable<Channel> forIds(Iterable<Long> ids) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Maybe<Channel> forAlias(String alias) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Iterable<Channel> allChannels(ChannelQuery query) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Iterable<Channel> forKeyPairAlias(ChannelQuery query) {
            throw new UnsupportedOperationException();
        }
    }
}

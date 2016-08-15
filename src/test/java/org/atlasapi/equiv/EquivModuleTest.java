package org.atlasapi.equiv;

import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.atlasapi.equiv.generators.BroadcastMatchingItemEquivalenceGenerator;
import org.atlasapi.equiv.generators.EquivalenceGenerator;
import org.atlasapi.equiv.generators.TitleSearchGenerator;
import org.atlasapi.equiv.handlers.PLEquivalenceResultHandler;
import org.atlasapi.equiv.query.MergeOnOutputQueryExecutor;
import org.atlasapi.equiv.results.combining.NullScoreAwareAveragingCombiner;
import org.atlasapi.equiv.results.extractors.PercentThresholdAboveNextBestMatchEquivalenceExtractor;
import org.atlasapi.equiv.results.filters.ConjunctiveFilter;
import org.atlasapi.equiv.results.filters.DummyContainerFilter;
import org.atlasapi.equiv.results.filters.EquivalenceFilter;
import org.atlasapi.equiv.results.filters.FilmFilter;
import org.atlasapi.equiv.results.filters.MediaTypeFilter;
import org.atlasapi.equiv.results.filters.MinimumScoreFilter;
import org.atlasapi.equiv.results.filters.PublisherFilter;
import org.atlasapi.equiv.results.filters.SpecializationFilter;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.scorers.*;
import org.atlasapi.equiv.scorers.proposedbroadcast.LDistanceTitleSubsetBroadcastItemScorer;
import org.atlasapi.equiv.scorers.proposedbroadcast.PL1TitleSubsetBroadcastItemScorer;
import org.atlasapi.equiv.scorers.proposedbroadcast.PLMatcherTitleSubsetBroadcastItemScorer;
import org.atlasapi.equiv.scorers.proposedbroadcast.PLStemmingTitleSubsetBroadcastItemScorer;
import org.atlasapi.equiv.scorers.proposedbroadcast.DescriptionTitleSubsetBroadcastItemScorer;
import org.atlasapi.equiv.scorers.proposedtitlematching.DescriptionMatching;
import org.atlasapi.equiv.scorers.proposedtitlematching.DescriptionTitleMatching;
import org.atlasapi.equiv.scorers.proposedtitlematching.LDistanceTitleMatching;
import org.atlasapi.equiv.update.ContentEquivalenceUpdater;
import org.atlasapi.equiv.update.EquivalenceUpdater;
import org.atlasapi.media.channel.ChannelGroupResolver;
import org.atlasapi.media.channel.ChannelGroupWriter;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.channel.MongoChannelGroupStore;
import org.atlasapi.media.channel.MongoChannelStore;
import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.messaging.v3.ScheduleUpdateMessage;
import org.atlasapi.persistence.audit.NoLoggingPersistenceAuditLog;
import org.atlasapi.persistence.audit.PersistenceAuditLog;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.DefaultEquivalentContentResolver;
import org.atlasapi.persistence.content.EquivalentContentResolver;
import org.atlasapi.persistence.content.KnownTypeContentResolver;
import org.atlasapi.persistence.content.LookupResolvingContentResolver;
import org.atlasapi.persistence.content.PeopleQueryResolver;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.persistence.content.ScheduleResolver;
import org.atlasapi.persistence.content.cassandra.CassandraContentStore;
import org.atlasapi.persistence.content.cassandra.CassandraKnownTypeContentResolver;
import org.atlasapi.persistence.content.mongo.MongoContentResolver;
import org.atlasapi.persistence.content.mongo.MongoPersonStore;
import org.atlasapi.persistence.content.people.EquivalatingPeopleResolver;
import org.atlasapi.persistence.content.people.IdSettingPersonStore;
import org.atlasapi.persistence.content.people.PersonStore;
import org.atlasapi.persistence.content.query.KnownTypeQueryExecutor;
import org.atlasapi.persistence.content.schedule.mongo.MongoScheduleStore;
import org.atlasapi.persistence.ids.MongoSequentialIdGenerator;
import org.atlasapi.persistence.lookup.TransitiveLookupWriter;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;
import org.atlasapi.persistence.lookup.mongo.MongoLookupEntryStore;
import org.atlasapi.persistence.shorturls.ShortUrlSaver;
import org.atlasapi.persistence.system.Fetcher;
import org.atlasapi.query.content.CurieResolvingQueryExecutor;
import org.atlasapi.query.content.FilterActivelyPublishedOnlyQueryExecutor;
import org.atlasapi.query.content.FilterScheduleOnlyQueryExecutor;
import org.atlasapi.query.content.LookupResolvingQueryExecutor;
import org.atlasapi.query.content.UriFetchingQueryExecutor;
import org.atlasapi.query.content.fuzzy.RemoteFuzzySearcher;
import org.atlasapi.query.content.search.ContentResolvingSearcher;
import org.atlasapi.query.uri.LocalOrRemoteFetcher;
import org.atlasapi.query.uri.canonical.Canonicaliser;
import org.atlasapi.query.uri.canonical.CanonicalisingFetcher;
import org.atlasapi.remotesite.bliptv.BlipTvAdapter;
import org.atlasapi.remotesite.dailymotion.DailyMotionItemAdapter;
import org.atlasapi.remotesite.facebook.FacebookCanonicaliser;
import org.atlasapi.remotesite.hulu.HuluItemAdapter;
import org.atlasapi.remotesite.hulu.WritingHuluBrandAdapter;
import org.atlasapi.remotesite.ted.TedTalkAdapter;
import org.atlasapi.remotesite.tinyurl.SavingShortUrlCanonicaliser;
import org.atlasapi.remotesite.tinyurl.ShortenedUrlCanonicaliser;
import org.atlasapi.remotesite.youtube.YouTubeFeedCanonicaliser;
import org.atlasapi.remotesite.youtube.YoutubeUriCanonicaliser;
import org.atlasapi.search.ContentSearcher;

import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.queue.kafka.KafkaMessageSender;
import com.metabroadcast.common.time.DateTimeZones;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.mongodb.Mongo;
import com.mongodb.ReadPreference;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.junit.Test;

import static org.atlasapi.media.entity.Publisher.*;
import static org.mockito.Mockito.mock;

public class EquivModuleTest {

    private static double DEFAULT_EXACT_TITLE_MATCH_SCORE = 2;

    private ScheduleResolver scheduleResolver;
    private ContentResolver contentResolver;
    private ChannelResolver channelResolver;
    private ContentResolvingSearcher searchResolver;
    private EquivalenceSummaryStore equivSummaryStore;
    private Set<Publisher> publisherSet;

    public EquivModuleTest() throws UnknownHostException {
        publisherSet = ImmutableSet.of(
                PREVIEW_NETWORKS,
                BBC_REDUX,
                RADIO_TIMES,
                LOVEFILM,
                NETFLIX,
                YOUVIEW,
                YOUVIEW_STAGE,
                YOUVIEW_BT,
                YOUVIEW_BT_STAGE,
                PA,
                YOUVIEW,
                BBC_NITRO,
                FIVE
        );

        Mongo mongo = new Mongo("db1.owl.atlas.mbst.tv");
        DatabasedMongo databasedMongo = new DatabasedMongo(mongo, "atlas-split");
        ChannelGroupResolver channelGroupResolver = new MongoChannelGroupStore(databasedMongo);
        ChannelGroupWriter channelGroupWriter = new MongoChannelGroupStore(databasedMongo);
        channelResolver = new MongoChannelStore(databasedMongo, channelGroupResolver, channelGroupWriter);
        PersistenceAuditLog persistenceAuditLog = mock(PersistenceAuditLog.class);
        LookupEntryStore lookupEntryStore = new MongoLookupEntryStore(databasedMongo.collection("lookup"), persistenceAuditLog, ReadPreference.secondary());
        KnownTypeContentResolver knownTypeContentResolver = new MongoContentResolver(databasedMongo,lookupEntryStore);
        contentResolver = new LookupResolvingContentResolver(knownTypeContentResolver, lookupEntryStore);
        EquivalentContentResolver equivalentContentResolver = new DefaultEquivalentContentResolver(knownTypeContentResolver, lookupEntryStore);
        KafkaMessageSender<ScheduleUpdateMessage> messageSender = mock(KafkaMessageSender.class);
        scheduleResolver = new MongoScheduleStore(databasedMongo, channelResolver, contentResolver, equivalentContentResolver, messageSender);
//        searchResolver = new SearchResolver();
        equivSummaryStore = mock(CassandraEquivalenceSummaryStore.class);
        ContentSearcher titleSearcher = new RemoteFuzzySearcher("http://search8.owl.atlas.mbst.tv");
        NoLoggingPersistenceAuditLog noLoggingPersistenceAuditLog = new NoLoggingPersistenceAuditLog();
        MongoLookupEntryStore lookupStore = new MongoLookupEntryStore(databasedMongo.collection("lookup"),
                noLoggingPersistenceAuditLog, ReadPreference.secondary());
        KnownTypeContentResolver mongoContentResolver = new MongoContentResolver(databasedMongo, lookupStore);
        AstyanaxContext<Keyspace> context = new AstyanaxContext.Builder()
                .forKeyspace("Atlas")
                .forCluster("Atlas")
                .withAstyanaxConfiguration(new AstyanaxConfigurationImpl().setDiscoveryType(
                        NodeDiscoveryType.NONE))
                .withConnectionPoolConfiguration(
                        new ConnectionPoolConfigurationImpl("Atlas")
                                .setPort(9160)
                                .setMaxBlockedThreadsPerHost(10)
                                .setMaxConnsPerHost(10)
                                .setConnectTimeout(60000)
                                .setSeeds("cassandra1.deer.atlas.mbst.tv,cassandra2.deer.atlas.mbst.tv,cassandra3.deer.atlas.mbst.tv")
                )
                .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
                .buildKeyspace(ThriftFamilyFactory.getInstance());
        CassandraContentStore cassandra = new CassandraContentStore(context, 60000);
        KnownTypeContentResolver cassandraContentResolver = new CassandraKnownTypeContentResolver(cassandra);

        KnownTypeQueryExecutor queryExecutor = new LookupResolvingQueryExecutor(cassandraContentResolver,
                mongoContentResolver, lookupStore, true);

        Fetcher<Identified> localOrRemoteFetcher = new LocalOrRemoteFetcher(contentResolver, mock(Fetcher.class));
        localOrRemoteFetcher=  new CanonicalisingFetcher(localOrRemoteFetcher, canonicalisers());

        queryExecutor = new UriFetchingQueryExecutor(localOrRemoteFetcher, queryExecutor, mock(EquivalenceUpdater.class), ImmutableSet.of(FACEBOOK));

        queryExecutor = new CurieResolvingQueryExecutor(queryExecutor);

        queryExecutor = new FilterActivelyPublishedOnlyQueryExecutor(queryExecutor);
        queryExecutor = new MergeOnOutputQueryExecutor(queryExecutor);
        queryExecutor = new FilterScheduleOnlyQueryExecutor(queryExecutor);
        LookupEntryStore personLookupEntryStore = new MongoLookupEntryStore(databasedMongo.collection("peopleLookup"),
                noLoggingPersistenceAuditLog, ReadPreference.secondary());
        PersonStore personStore = new MongoPersonStore(databasedMongo,
                TransitiveLookupWriter.explicitTransitiveLookupWriter(personLookupEntryStore),
                personLookupEntryStore,
                noLoggingPersistenceAuditLog);

        //For now people occupy the same id space as content.
        personStore = new IdSettingPersonStore(
                personStore, new MongoSequentialIdGenerator(databasedMongo, "content")
        );
        PeopleQueryResolver resolver = new EquivalatingPeopleResolver(personStore, new MongoLookupEntryStore(databasedMongo.collection("peopleLookup"),
                noLoggingPersistenceAuditLog, ReadPreference.secondary()));
        searchResolver =  new ContentResolvingSearcher(titleSearcher, queryExecutor, resolver);
    }

    public List<Canonicaliser> canonicalisers() {
        List<Canonicaliser> canonicalisers = Lists.newArrayList();
        canonicalisers.add(new YoutubeUriCanonicaliser());
        canonicalisers.add(new YouTubeFeedCanonicaliser());
        canonicalisers.add(new FacebookCanonicaliser());
        canonicalisers.add(new TedTalkAdapter.TedTalkCanonicaliser());
        canonicalisers.add(new DailyMotionItemAdapter.DailyMotionItemCanonicaliser());
        canonicalisers.add(new BlipTvAdapter.BlipTvCanonicaliser());
        canonicalisers.add(new HuluItemAdapter.HuluItemCanonicaliser());
        canonicalisers.add(new WritingHuluBrandAdapter.HuluBrandCanonicaliser());
        canonicalisers.add(new SavingShortUrlCanonicaliser(new ShortenedUrlCanonicaliser(),  mock(ShortUrlSaver.class)));
        return canonicalisers;
    }


    private static Predicate<Broadcast> YOUVIEW_BROADCAST_FILTER = new Predicate<Broadcast>() {

        @Override
        public boolean apply(Broadcast input) {
            DateTime twoWeeksAgo = new DateTime(DateTimeZones.UTC).minusDays(15);
            return input.getTransmissionTime().isAfter(twoWeeksAgo);
        }
    };

    @Test
    public void testPL1() throws Exception {
        runTest(new PL1TitleSubsetBroadcastItemScorer(contentResolver, Score.nullScore(), 80/*percent*/), "PL1", false);
    }

    @Test
    public void testPLMatcher() throws Exception {
        runTest(new PLMatcherTitleSubsetBroadcastItemScorer(contentResolver, Score.nullScore(), 80/*percent*/), "PLMatcher", false);
    }

    @Test
    public void testPLStemming() throws Exception {
        runTest(new PLStemmingTitleSubsetBroadcastItemScorer(contentResolver, Score.nullScore(), 80/*percent*/), "PLStemming", false);
    }

    @Test
    public void testLDistance() throws Exception {
        runTest(new LDistanceTitleSubsetBroadcastItemScorer(contentResolver, Score.nullScore(), 80/*percent*/), "PLLDistance", false);
    }

    @Test
    public void testOriginal() throws Exception {
        runTest(new TitleSubsetBroadcastItemScorer(contentResolver, Score.nullScore(), 80/*percent*/), "Original", false);
    }

    @Test
    public void testPL1PlusDescription() throws Exception {
        runTest(new PL1TitleSubsetBroadcastItemScorer(contentResolver, Score.nullScore(), 80/*percent*/), "DescriptionPlusPL1", true);
    }

    @Test
    public void testPLMatcherPlusDescription() throws Exception {
        runTest(new PLMatcherTitleSubsetBroadcastItemScorer(contentResolver, Score.nullScore(), 80/*percent*/), "DescriptionPlusPLMatcher", true);
    }

    @Test
    public void testPLStemmingPlusDescription() throws Exception {
        runTest(new PLStemmingTitleSubsetBroadcastItemScorer(contentResolver, Score.nullScore(), 80/*percent*/), "DescriptionPlusPLStemming", true);
    }

    @Test
    public void testLDistancePlusDescription() throws Exception {
        runTest(new LDistanceTitleSubsetBroadcastItemScorer(contentResolver, Score.nullScore(), 80/*percent*/), "DescriptionPlusPLLDistance", true);
    }

    @Test
    public void testOriginalPlusDescription() throws Exception {
        runTest(new TitleSubsetBroadcastItemScorer(contentResolver, Score.nullScore(), 80/*percent*/), "DescriptionPlusOriginal", true);
    }

    @Test
    public void testTitleMLDistance() throws Exception {
        runTest(new LDistanceTitleMatching(), "TitleMLDistance", false);
    }

    @Test
    public void testTitleMLDistancePlusDescription() throws Exception {
        runTest(new LDistanceTitleMatching(), "DescriptionPlusTitleMLDistance", true);
    }


    public void runTest(EquivalenceScorer<Item> baseScorer, String titleAddOn, boolean descriptionScorer) throws Exception {
        EquivalenceUpdater<Item> equivalenceUpdater = broadcastItemEquivalenceUpdater(
                publisherSet,
                Score.negativeOne(),
                YOUVIEW_BROADCAST_FILTER,
                baseScorer,
                titleAddOn,
                descriptionScorer
        );


        ResolvedContent resolvedContent = contentResolver.findByUris(getContentList());

        for (String contentString: getContentList()) {
            Item content = (Item) resolvedContent.get(contentString).requireValue();

            equivalenceUpdater.updateEquivalences(content);
        }

    }

    private EquivalenceUpdater<Item> broadcastItemEquivalenceUpdater(Set<Publisher> sources, Score titleMismatch,
            Predicate<? super Broadcast> filter, EquivalenceScorer<Item> baseScorer, String titleAddOn, boolean descriptionScorer) {
        if (descriptionScorer) {
            return standardItemUpdater(sources, ImmutableSet.of(
//                    new TitleMatchingItemScorer(),
                    new SequenceItemScorer(Score.ONE),
                    new DescriptionTitleMatching(),
//                    new DescriptionMatching(),
                    baseScorer,
                    new BroadcastAliasScorer(Score.negativeOne())
            ), filter, titleAddOn).build();
        } else {
            return standardItemUpdater(sources, ImmutableSet.of(
//                    new TitleMatchingItemScorer(),
                    new SequenceItemScorer(Score.ONE),
                    baseScorer,
                    new BroadcastAliasScorer(Score.negativeOne())
            ), filter, titleAddOn).build();
        }
    }


    private ContentEquivalenceUpdater.Builder<Item> standardTitleSearchItemUpdater(Set<Publisher> acceptablePublishers,
            Set<? extends EquivalenceScorer<Item>> scorers, Predicate<? super Broadcast> filter, String titleAddOn) {
        return ContentEquivalenceUpdater.<Item>builder()
                .withGenerators(ImmutableSet.of(TitleSearchGenerator.create(searchResolver, Item.class, acceptablePublishers, DEFAULT_EXACT_TITLE_MATCH_SCORE)))
                .withExcludedUris(ImmutableSet.of())
                .withScorers(scorers)
                .withCombiner(new NullScoreAwareAveragingCombiner<>())
                .withFilter(this.standardFilter())
                .withExtractor(PercentThresholdAboveNextBestMatchEquivalenceExtractor.atLeastNTimesGreater(1.5))
                .withHandler(
                        new PLEquivalenceResultHandler(titleAddOn)
                );
    }


    private ContentEquivalenceUpdater.Builder<Item> standardItemUpdater(Set<Publisher> acceptablePublishers,
            Set<? extends EquivalenceScorer<Item>> scorers, Predicate<? super Broadcast> filter, String titleAddOn) {
        return ContentEquivalenceUpdater.<Item>builder()
                .withGenerators(ImmutableSet.<EquivalenceGenerator<Item>> of(
                        new BroadcastMatchingItemEquivalenceGenerator(scheduleResolver,
                                channelResolver, acceptablePublishers, Duration.standardMinutes(5), filter)
                ))
                .withExcludedUris(ImmutableSet.of())
                .withScorers(scorers)
                .withCombiner(new NullScoreAwareAveragingCombiner<>())
                .withFilter(this.standardFilter())
                .withExtractor(PercentThresholdAboveNextBestMatchEquivalenceExtractor.atLeastNTimesGreater(1.5))
                .withHandler(
                        new PLEquivalenceResultHandler(titleAddOn)
                );
    }

    private <T extends Content> EquivalenceFilter<T> standardFilter() {
        return ConjunctiveFilter.valueOf(Iterables.concat(ImmutableList.of(
                new MinimumScoreFilter<T>(0.2),
                new MediaTypeFilter<T>(),
                new SpecializationFilter<T>(),
                new PublisherFilter<T>(),
                new FilmFilter<T>(),
                new DummyContainerFilter<T>()
        ), ImmutableList.<EquivalenceFilter<T>>of()));
    }

    private List<String> getContentList() {
        List<String> contentList = new LinkedList<String>();
//        contentList.add("http://youview.com/programmecrid/20160729/movies4men.co.uk/E2713312"); //no title but same description
//        contentList.add("http://youview.com/programmecrid/20160729/movies4men.co.uk/E2707926"); //no title but same description
//        contentList.add("http://youview.com/programmecrid/20160716/movies4men.co.uk/E1913523"); //no title but same description
//        //Similar spelling/ american spelling
//        contentList.add("http://radiotimes.com/films/53183");
//        contentList.add("http://youview.com/scheduleevent/33031768");
//        //mismatches in search
//        //further language changes
//        contentList.add("http://itv.com/1046766");
//        contentList.add("http://youview.com/programmecrid/20151215/fp.bbc.co.uk/A7ETND");
//        contentList.add("http://youview.com/scheduleevent/32911802");
//        contentList.add("https://pdb.five.tv/internal/watchables/C5155250122");
//        contentList.add("http://youview.com/programmecrid/20150716/fp.bbc.co.uk/AX01S1");
//        contentList.add("http://youview.com/scheduleevent/33114169");
//        contentList.add("http://youview.com/scheduleevent/33729396");
//        contentList.add("http://youview.com/scheduleevent/17319225");
//
//
//        contentList.add("http://youview.com/scheduleevent/17813416");
//        contentList.add("http://youview.com/programmecrid/20151113/fp.bbc.co.uk/DBYGGX");
//        //contentList.add("http://bt.youview.com/programmecrid/20160725/bds.tv/138153538"); // motives and murder - wrong type
//
//        contentList.add("http://youview.com/programmecrid/20160606/www.channel4.com/52096/019");
//        contentList.add("http://youview.com/programmecrid/20160720/www.channel4.com/48467/023");
//        contentList.add("http://youview.com/programmecrid/20160708/www.channel4.com/48467/008");
//        contentList.add("http://g.bbcredux.com/programme/6062016485369359592");
//        contentList.add("http://youview.com/programmecrid/20160708/www.channel4.com/48467/009");
//        contentList.add("http://g.bbcredux.com/programme/6088332608985424265");
//        contentList.add("http://youview.com/programmecrid/20160306/www.channel4.com/39876/043"); // come die with me? misspelling worth checking equiv accuracy
////
////
////        // No Broadcasts
//        contentList.add("http://priorities.metabroadcast.com/d877js");
//        contentList.add("http://unbox.amazon.co.uk/B00HV8XEUW");
//        contentList.add("http://itunes.apple.com/video/id473589343");
//        contentList.add("http://itunes.apple.com/video/id943873419");
//        contentList.add("http://vod.bt.com/items/BBJ819052A");
//        contentList.add("http://p06.pmlsc.channel4.com/pmlsd/39876/025");
//
//
//        //count = 28
//
//
//        contentList.add("http://youview.com/scheduleevent/33858911");
//        contentList.add("http://youview.com/scheduleevent/33786584");
//        contentList.add("http://youview.com/scheduleevent/33902443");
//        contentList.add("http://youview.com/scheduleevent/33786579");
//        contentList.add("http://youview.com/scheduleevent/33786583");
//        contentList.add("http://youview.com/scheduleevent/33884514");
//        contentList.add("http://youview.com/scheduleevent/33902507");
//        contentList.add("http://youview.com/scheduleevent/33902504");
//        contentList.add("http://youview.com/scheduleevent/33786396");
//        contentList.add("http://youview.com/scheduleevent/33902333");
//        contentList.add("http://youview.com/scheduleevent/33902331");


//        contentList.add("https://pdb.five.tv/internal/watchables/C5180710038"); // sport with mismatched titles but matching description

        contentList.add("http://youview.com/scheduleevent/33813181");

        return contentList;
    }
}
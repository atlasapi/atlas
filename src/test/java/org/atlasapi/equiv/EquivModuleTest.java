package org.atlasapi.equiv;

import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.atlasapi.application.v3.ApplicationConfiguration;
import org.atlasapi.equiv.generators.BroadcastMatchingItemEquivalenceGenerator;
import org.atlasapi.equiv.generators.EquivalenceGenerator;
import org.atlasapi.equiv.handlers.PLEquivalenceResultHandler;
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
import org.atlasapi.equiv.scorers.BaseBroadcastItemScorer;
import org.atlasapi.equiv.scorers.BroadcastAliasScorer;
import org.atlasapi.equiv.scorers.EquivalenceScorer;
import org.atlasapi.equiv.scorers.LDistanceTitleSubsetBroadcastItemScorer;
import org.atlasapi.equiv.scorers.PL1TitleSubsetBroadcastItemScorer;
import org.atlasapi.equiv.scorers.PLMatcherTitleSubsetBroadcastItemScorer;
import org.atlasapi.equiv.scorers.PLStemmingTitleSubsetBroadcastItemScorer;
import org.atlasapi.equiv.scorers.SequenceItemScorer;
import org.atlasapi.equiv.scorers.TitleMatchingItemScorer;
import org.atlasapi.equiv.scorers.TitleSubsetBroadcastItemScorer;
import org.atlasapi.equiv.update.ContentEquivalenceUpdater;
import org.atlasapi.equiv.update.EquivalenceUpdater;
import org.atlasapi.media.channel.Channel;
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
import org.atlasapi.media.entity.Schedule;
import org.atlasapi.messaging.v3.ScheduleUpdateMessage;
import org.atlasapi.persistence.audit.PersistenceAuditLog;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.DefaultEquivalentContentResolver;
import org.atlasapi.persistence.content.EquivalentContentResolver;
import org.atlasapi.persistence.content.KnownTypeContentResolver;
import org.atlasapi.persistence.content.LookupResolvingContentResolver;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.persistence.content.ScheduleResolver;
import org.atlasapi.persistence.content.SearchResolver;
import org.atlasapi.persistence.content.mongo.MongoContentResolver;
import org.atlasapi.persistence.content.schedule.mongo.MongoScheduleStore;
import org.atlasapi.persistence.lookup.LookupWriter;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;
import org.atlasapi.persistence.lookup.mongo.MongoLookupEntryStore;

import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.queue.MessageSenderFactory;
import com.metabroadcast.common.queue.kafka.KafkaMessageSender;
import com.metabroadcast.common.time.DateTimeZones;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.mongodb.Mongo;
import com.mongodb.ReadPreference;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.junit.Test;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.validateMockitoUsage;

import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;


public class EquivModuleTest {

    private ScheduleResolver scheduleResolver;
    private ContentResolver contentResolver;
    private ChannelResolver channelResolver;

    public EquivModuleTest() throws UnknownHostException {
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
        runTest(new PL1TitleSubsetBroadcastItemScorer(contentResolver, Score.negativeOne(), 80/*percent*/), "PL1");
    }

    @Test
    public void testPLMatcher() throws Exception {
        runTest(new PLMatcherTitleSubsetBroadcastItemScorer(contentResolver, Score.negativeOne(), 80/*percent*/), "PLMatcher");
    }

    @Test
    public void testPLStemming() throws Exception {
        runTest(new PLStemmingTitleSubsetBroadcastItemScorer(contentResolver, Score.negativeOne(), 80/*percent*/), "PLStemming");
    }

    @Test
    public void testLDistance() throws Exception {
        runTest(new LDistanceTitleSubsetBroadcastItemScorer(contentResolver, Score.negativeOne(), 80/*percent*/), "PLLDistance");
    }

    @Test
    public void testOriginal() throws Exception {
        runTest(new TitleSubsetBroadcastItemScorer(contentResolver, Score.negativeOne(), 80/*percent*/), "Original");
    }


    public void runTest(BaseBroadcastItemScorer baseScorer, String titleAddOn) throws Exception {
        EquivalenceUpdater<Item> equivalenceUpdater = broadcastItemEquivalenceUpdater(
                ImmutableSet.of(Publisher.PA), //TODO ADD A TARGET
                Score.negativeOne(),
                YOUVIEW_BROADCAST_FILTER,
                baseScorer,
                titleAddOn
        );


        ResolvedContent resolvedContent = contentResolver.findByUris(getContentList());

        for (String contentString: getContentList()) {
            Item content = (Item) resolvedContent.get(contentString).requireValue();

            if (content.getVersions().isEmpty()
                    || content.getVersions()
                    .stream()
                    .flatMap(version -> version.getBroadcasts().stream())
                    .count() == 0) {
                System.out.println(content.getCanonicalUri());
            }

//            equivalenceUpdater.updateEquivalences(content);
        }

    }

    private EquivalenceUpdater<Item> broadcastItemEquivalenceUpdater(Set<Publisher> sources, Score titleMismatch,
            Predicate<? super Broadcast> filter, BaseBroadcastItemScorer baseScorer, String titleAddOn) {
        return standardItemUpdater(sources, ImmutableSet.of(
                new TitleMatchingItemScorer(),
                new SequenceItemScorer(Score.ONE),
                baseScorer,
                new BroadcastAliasScorer(Score.nullScore())
        ), filter, titleAddOn).build();
    }

    // TODO check publishers settings in EquivModule
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
        contentList.add("http://youview.com/programmecrid/20160729/movies4men.co.uk/E2713312"); //no title but same description
        contentList.add("http://youview.com/programmecrid/20160729/movies4men.co.uk/E2707926"); //no title but same description
        contentList.add("http://youview.com/programmecrid/20160716/movies4men.co.uk/E1913523"); //no title but same description
        //Similar spelling/ american spelling
        contentList.add("http://radiotimes.com/films/53183");
        contentList.add("http://youview.com/scheduleevent/33031768");
        //mismatches in search
        //further language changes
        contentList.add("http://itv.com/1046766");
        contentList.add("http://youview.com/programmecrid/20151215/fp.bbc.co.uk/A7ETND");
        contentList.add("http://youview.com/scheduleevent/32911802");
        contentList.add("https://pdb.five.tv/internal/watchables/C5155250122");
        contentList.add("http://youview.com/programmecrid/20150716/fp.bbc.co.uk/AX01S1");
        contentList.add("http://youview.com/scheduleevent/33114169");
        contentList.add("http://youview.com/scheduleevent/33729396");
        contentList.add("http://youview.com/scheduleevent/17319225");


        //STILL NEED TO BE PURGED
        contentList.add("http://youview.com/scheduleevent/17813416");
        contentList.add("http://youview.com/programmecrid/20151113/fp.bbc.co.uk/DBYGGX");
        //contentList.add("http://bt.youview.com/programmecrid/20160725/bds.tv/138153538"); // motives and murder - wrong type

        contentList.add("http://youview.com/programmecrid/20160606/www.channel4.com/52096/019");
        contentList.add("http://youview.com/programmecrid/20160720/www.channel4.com/48467/023");
        contentList.add("http://youview.com/programmecrid/20160708/www.channel4.com/48467/008");
        contentList.add("http://g.bbcredux.com/programme/6062016485369359592");
        contentList.add("http://youview.com/programmecrid/20160708/www.channel4.com/48467/009");
        contentList.add("http://g.bbcredux.com/programme/6088332608985424265");
        contentList.add("http://youview.com/programmecrid/20160306/www.channel4.com/39876/043");


        // No Broadcasts
        contentList.add("http://priorities.metabroadcast.com/d877js");
        contentList.add("http://unbox.amazon.co.uk/B00HV8XEUW");
        contentList.add("http://itunes.apple.com/video/id473589343");
        contentList.add("http://itunes.apple.com/video/id943873419");
        contentList.add("http://vod.bt.com/items/BBJ819052A");
        contentList.add("http://p06.pmlsc.channel4.com/pmlsd/39876/025");


        //count = 28



        return contentList;
    }
}
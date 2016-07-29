package org.atlasapi.equiv;

import java.net.UnknownHostException;
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
import org.atlasapi.equiv.scorers.BroadcastAliasScorer;
import org.atlasapi.equiv.scorers.EquivalenceScorer;
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
        Mongo mongo = new Mongo("db1.stage.atlas.mbst.tv");
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
    public void runTest() throws Exception {
        EquivalenceUpdater<Item> equivalenceUpdater = broadcastItemEquivalenceUpdater(
                ImmutableSet.of(Publisher.PA),
                Score.negativeOne(),
                YOUVIEW_BROADCAST_FILTER
        );

        String uriToTest = "http://bt.youview.com/programmecrid/20160725/bds.tv/138153538";

        ResolvedContent resolvedContent = contentResolver.findByUris(ImmutableList.of(uriToTest));
        Item content = (Item) resolvedContent.get(uriToTest).requireValue();

        equivalenceUpdater.updateEquivalences(content);
    }

    private EquivalenceUpdater<Item> broadcastItemEquivalenceUpdater(Set<Publisher> sources, Score titleMismatch,
            Predicate<? super Broadcast> filter) {
        return standardItemUpdater(sources, ImmutableSet.of(
                new TitleMatchingItemScorer(),
                new SequenceItemScorer(Score.ONE),
                new PLStemmingTitleSubsetBroadcastItemScorer(contentResolver, titleMismatch, 80/*percent*/),
                new BroadcastAliasScorer(Score.nullScore())
        ), filter).build();
    }

    private ContentEquivalenceUpdater.Builder<Item> standardItemUpdater(Set<Publisher> acceptablePublishers,
            Set<? extends EquivalenceScorer<Item>> scorers, Predicate<? super Broadcast> filter) {
        return ContentEquivalenceUpdater.<Item> builder()
                .withGenerators(ImmutableSet.<EquivalenceGenerator<Item>> of(
                        new BroadcastMatchingItemEquivalenceGenerator(scheduleResolver,
                                channelResolver, acceptablePublishers, Duration.standardMinutes(5), filter)
                ))
                .withExcludedUris(ImmutableSet.of())
                .withScorers(scorers)
                .withCombiner(new NullScoreAwareAveragingCombiner<Item>())
                .withFilter(this.<Item>standardFilter())
                .withExtractor(PercentThresholdAboveNextBestMatchEquivalenceExtractor.<Item> atLeastNTimesGreater(1.5))
                .withHandler(
                        new PLEquivalenceResultHandler()
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
}
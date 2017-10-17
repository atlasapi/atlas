package org.atlasapi.equiv.generators;

import static org.atlasapi.media.entity.Publisher.BBC;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.joda.time.Duration.standardMinutes;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import org.atlasapi.equiv.results.description.DefaultDescription;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.MediaType;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Schedule;
import org.atlasapi.media.entity.Version;
import org.atlasapi.persistence.content.ScheduleResolver;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.time.DateTimeZones;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class BroadcastMatchingItemEquivalenceGeneratorTest extends TestCase {

    private static final Channel BBC_ONE = new Channel(Publisher.METABROADCAST, "BBC One", "bbcone", false, MediaType.AUDIO, "http://www.bbc.co.uk/bbcone");
    private static final Channel BBC_ONE_CAMBRIDGE = new Channel(Publisher.METABROADCAST, "BBC One Cambridgeshire", "bbcone-cambridge", false, MediaType.AUDIO, "http://www.bbc.co.uk/services/bbcone/cambridge");

    private final ScheduleResolver resolver = mock(ScheduleResolver.class);
    private BroadcastMatchingItemEquivalenceGenerator generator;
    
    @Before
    public void setUp() {
    	final ChannelResolver channelResolver = mock(ChannelResolver.class);

        when(channelResolver.fromUri(BBC_ONE.getUri())).thenReturn(Maybe.just(BBC_ONE));
        when(channelResolver.fromUri(BBC_ONE_CAMBRIDGE.getUri())).thenReturn(Maybe.just(BBC_ONE_CAMBRIDGE));

    	generator = new BroadcastMatchingItemEquivalenceGenerator(resolver, channelResolver, ImmutableSet.of(BBC), standardMinutes(1));
    }

    @Test
    public void testGenerateEquivalencesForOneMatchingBroadcast() {
        final Item item1 = episodeWithBroadcasts("subjectItem", Publisher.PA, 
                new Broadcast(BBC_ONE.getUri(), utcTime(100000), utcTime(2000000)),
                new Broadcast(BBC_ONE_CAMBRIDGE.getUri(), utcTime(100000), utcTime(2000000)));//ignored
        
        final Item item2 = episodeWithBroadcasts("equivItem", Publisher.BBC, new Broadcast(BBC_ONE.getUri(), utcTime(100000), utcTime(2000000)));

        when(resolver.unmergedSchedule(utcTime(40000), utcTime(2060000), ImmutableSet.of(BBC_ONE), ImmutableSet.of(BBC)))
                .thenReturn(Schedule.fromChannelMap(ImmutableMap.of(BBC_ONE, (List<Item>)ImmutableList.<Item>of(item2)), interval(40000, 2060000)));

        ScoredCandidates<Item> equivalents = generator.generate(item1, new DefaultDescription());
        
        Map<Item, Score> scoreMap = equivalents.candidates();
        
        assertThat(scoreMap.size(), is(1));
        assertThat(scoreMap.get(item2).asDouble(), is(equalTo(1.0)));
    }

    /**
     * If the only broadcast is one on an ignored channel, then we shouldn't ignore it; otherwise
     * we'll not compute equivalence for broadcast-based publishers (redux, youview) on ignored
     * channels, since they'll not be computed from other broadcasts of the item.
     */
    @Test
    public void testGenerateEquivalenceForRegionalVariantWhenOnlyBroadcast() {
        final Item item1 = episodeWithBroadcasts("subjectItem", Publisher.PA, 
                new Broadcast(BBC_ONE_CAMBRIDGE.getUri(), utcTime(100000), utcTime(2000000)));//not ignored
        
        final Item item2 = episodeWithBroadcasts("equivItem", Publisher.BBC, new Broadcast(BBC_ONE_CAMBRIDGE.getUri(), utcTime(100000), utcTime(2000000)));
        
            when(resolver.unmergedSchedule(utcTime(40000), utcTime(2060000), ImmutableSet.of(BBC_ONE_CAMBRIDGE), ImmutableSet.of(BBC)))
                    .thenReturn(Schedule.fromChannelMap(ImmutableMap.of(BBC_ONE_CAMBRIDGE, (List<Item>)ImmutableList.<Item>of(item2)), interval(40000, 2060000)));

        ScoredCandidates<Item> equivalents = generator.generate(item1, new DefaultDescription());
        
        Map<Item, Score> scoreMap = equivalents.candidates();
        
        assertThat(scoreMap.size(), is(1));
        assertThat(scoreMap.get(item2).asDouble(), is(equalTo(1.0)));
    }
    
    @Test
    public void testGenerateIsFlexibleAroundStartTimes() {
        
        final ChannelResolver channelResolver = mock(ChannelResolver.class, "otherChannelResolver");
        
        when(channelResolver.fromUri(BBC_ONE.getUri())).thenReturn(Maybe.just(BBC_ONE));

        BroadcastMatchingItemEquivalenceGenerator generator
            = new BroadcastMatchingItemEquivalenceGenerator(resolver, channelResolver, ImmutableSet.of(BBC), standardMinutes(10));
        
        
        final Item item1 = episodeWithBroadcasts("subjectItem", Publisher.PA, 
                new Broadcast(BBC_ONE.getUri(), time("2014-03-21T15:00:00Z"), time("2014-03-21T15:50:00Z")));
        
        final Item item2 = episodeWithBroadcasts("equivItem", Publisher.BBC, 
                new Broadcast(BBC_ONE.getUri(), time("2014-03-21T15:00:00Z"), time("2014-03-21T16:00:00Z")));
        
        when(resolver.unmergedSchedule(time("2014-03-21T14:50:00Z"), time("2014-03-21T16:00:00Z"), ImmutableSet.of(BBC_ONE), ImmutableSet.of(BBC)))
                .thenReturn(Schedule.fromChannelMap(ImmutableMap.of(BBC_ONE, (List<Item>)ImmutableList.<Item>of(item2)), interval(40000, 260000)));

        ScoredCandidates<Item> equivalents = generator.generate(item1, new DefaultDescription());
        
        Map<Item, Score> scoreMap = equivalents.candidates();
        
        assertThat(scoreMap.size(), is(1));
        assertThat(scoreMap.get(item2).asDouble(), is(equalTo(1.0)));
    }
    
    private Interval interval(long startMillis, long endMillis) {
        return new Interval(startMillis, endMillis, DateTimeZones.UTC);
    }
    
    private DateTime utcTime(long millis) {
        return new DateTime(millis, DateTimeZones.UTC);
    }

    private DateTime time(String date) {
        return new DateTime(date);
    }
    
    private Episode episodeWithBroadcasts(String episodeId, Publisher publisher, Broadcast... broadcasts) {
        Episode item = new Episode(episodeId+"Uri", episodeId+"Curie", publisher);
        Version version = new Version();
        version.setCanonicalUri(episodeId+"Version");
        version.setProvider(publisher);
        for (Broadcast broadcast : broadcasts) {
            version.addBroadcast(broadcast);
        }
        item.addVersion(version);
        return item;
    }

    @Test
    public void broadcastsWithDurationLessThanTenMinutesHaveLowerFlexibility() {
        Item item1 = episodeWithBroadcasts("subjectitem", Publisher.PA,
                new Broadcast(BBC_ONE_CAMBRIDGE.getUri(), utcTime(1000000), utcTime(1200000)));//not ignored

        Item item2 = episodeWithBroadcasts("equivitem", Publisher.BBC, new Broadcast(BBC_ONE_CAMBRIDGE.getUri(), utcTime(1000000), utcTime(1200000)));

        when(resolver.unmergedSchedule(utcTime(880000), utcTime(1320000), ImmutableSet.of(BBC_ONE_CAMBRIDGE), ImmutableSet.of(BBC)))
                .thenReturn(Schedule.fromChannelMap(ImmutableMap.of(BBC_ONE_CAMBRIDGE, (List<Item>)ImmutableList.<Item>of(item2)), interval(880000, 1320000)));

        ScoredCandidates<Item> equivalents = generator.generate(item1, new DefaultDescription());

        Map<Item, Score> scoreMap = equivalents.candidates();

        assertThat(scoreMap.size(), is(1));
        assertThat(scoreMap.get(item2).asDouble(), is(equalTo(1.0)));
    }
}

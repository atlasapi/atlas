package org.atlasapi.equiv.generators.barb;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.stream.MoreCollectors;
import org.atlasapi.equiv.generators.barb.utils.BarbGeneratorUtils;
import org.atlasapi.equiv.results.description.DefaultDescription;
import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.equiv.scorers.barb.BarbTitleMatchingItemScorer;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResult;
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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.atlasapi.equiv.generators.barb.TieredBroadcaster.TXLOG_BROADCASTER_GROUP;
import static org.atlasapi.media.entity.Publisher.BARB_TRANSMISSIONS;
import static org.atlasapi.media.entity.Publisher.BBC;
import static org.atlasapi.media.entity.Publisher.BBC_NITRO;
import static org.atlasapi.media.entity.Publisher.PA;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.joda.time.Duration.standardHours;
import static org.joda.time.Duration.standardMinutes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BarbBroadcastMatchingItemEquivalenceGeneratorAndScorerTest {

    private static final Channel BBC_ONE = new Channel(
            Publisher.METABROADCAST,
            "BBC One",
            "bbcone",
            false,
            MediaType.AUDIO,
            "http://www.bbc.co.uk/bbcone"
    );
    private static final Channel BBC_ONE_CAMBRIDGE = new Channel(
            Publisher.METABROADCAST,
            "BBC One Cambridgeshire",
            "bbcone-cambridge",
            false,
            MediaType.AUDIO,
            "http://www.bbc.co.uk/services/bbcone/cambridge"
    );

    public static final Channel BBC_TWO_ENGLAND = new Channel(
            Publisher.BBC_NITRO,
            "BBC Two England",
            "bbctwo-england",
            false,
            MediaType.AUDIO,
            "http://www.bbc.co.uk/services/bbctwo/england"
    );

    public static final Map<String, Channel> BBC_TWO_ENGLAND_TXLOG_CHANNEL_MAP =
            BarbGeneratorUtils
                    .CHANNELS_WITH_MULTIPLE_TXLOG_CHANNEL_VARIANTS.get(BBC_TWO_ENGLAND.getUri())
                    .stream()
                    .map(uri ->
                            new Channel(
                                    Publisher.METABROADCAST,
                                    uri,
                                    uri,
                                    false,
                                    MediaType.AUDIO,
                                    uri
                            ))
                    .collect(MoreCollectors.toImmutableMap(Channel::getUri, channel -> channel));

    public static final Channel BBC_TWO_SOUTH_TXLOG =
            BBC_TWO_ENGLAND_TXLOG_CHANNEL_MAP.get("http://channels.barb.co.uk/channels/1085");
    public static final Channel BBC_TWO_EAST_TXLOG =
            BBC_TWO_ENGLAND_TXLOG_CHANNEL_MAP.get("http://channels.barb.co.uk/channels/1082");


    private static final Set<Publisher> PUBLISHERS = ImmutableSet.of(BBC, BBC_NITRO, BARB_TRANSMISSIONS);
    private static final Score SCORE_ON_MATCH = Score.ONE;

    private final ChannelResolver channelResolver = mock(ChannelResolver.class);
    private final ScheduleResolver resolver = mock(ScheduleResolver.class);
    private final BarbTitleMatchingItemScorer titleMatchingScorer = mock(BarbTitleMatchingItemScorer.class);
    private BarbBroadcastMatchingItemEquivalenceGeneratorAndScorer generator;

    @Before
    public void setUp() {

        when(channelResolver.fromUri(BBC_ONE.getUri())).thenReturn(Maybe.just(BBC_ONE));
        when(channelResolver.fromUri(BBC_ONE_CAMBRIDGE.getUri())).thenReturn(Maybe.just(BBC_ONE_CAMBRIDGE));
        when(channelResolver.fromUri(BBC_TWO_ENGLAND.getUri())).thenReturn(Maybe.just(BBC_TWO_ENGLAND));
        for (Channel channel : BBC_TWO_ENGLAND_TXLOG_CHANNEL_MAP.values()) {
            when(channelResolver.fromUri(channel.getUri())).thenReturn(Maybe.just(channel));
        }

        when(titleMatchingScorer.score(any(Item.class), any(Item.class), any(ResultDescription.class)))
                .thenReturn(Score.nullScore());

        generator = BarbBroadcastMatchingItemEquivalenceGeneratorAndScorer.builder()
                .withScheduleResolver(resolver)
                .withChannelResolver(channelResolver)
                .withSupportedPublishers(PUBLISHERS)
                .withScheduleWindow(standardHours(1))
                .withScoreOnMatch(SCORE_ON_MATCH)
                .withBroadcastFlexibility(standardMinutes(10))
                .withShortBroadcastFlexibility(standardMinutes(2))
                .withShortBroadcastMaxDuration(standardMinutes(10))
                .withTitleMatchingScorer(titleMatchingScorer)
                .build();
    }

    @Test
    public void testGenerateEquivalencesForOneMatchingBroadcast() {
        final Item item1 = episodeWithBroadcasts("subjectItem", Publisher.PA,
                new Broadcast(BBC_ONE.getUri(), time("2014-03-21T15:00:00Z"), time("2014-03-21T16:00:00Z")),
                new Broadcast(BBC_ONE_CAMBRIDGE.getUri(), time("2014-03-21T15:00:00Z"), time("2014-03-21T16:00:00Z"))
        );
        item1.addCustomField(TXLOG_BROADCASTER_GROUP, "1"); // since we now ignore tier 2 broadcasts

        final Item item2 = episodeWithBroadcasts(
                "equivItem",
                BBC,
                new Broadcast(BBC_ONE.getUri(), time("2014-03-21T15:00:00Z"), time("2014-03-21T16:00:00Z"))
        );

        setupScheduleResolving("2014-03-21T14:00:00Z", "2014-03-21T17:00:00Z", BBC_ONE, PA, item1);
        setupScheduleResolving("2014-03-21T14:00:00Z", "2014-03-21T17:00:00Z", BBC_ONE_CAMBRIDGE, PA, item1);
        setupScheduleResolving("2014-03-21T14:00:00Z", "2014-03-21T17:00:00Z", BBC_ONE, PUBLISHERS, item2);
        setupScheduleResolving("2014-03-21T14:00:00Z", "2014-03-21T17:00:00Z", BBC_ONE_CAMBRIDGE, PUBLISHERS);

        setupTitleScoring(item1, item2, SCORE_ON_MATCH);

        ScoredCandidates<Item> equivalents = generator.generate(
                item1,
                new DefaultDescription(),
                EquivToTelescopeResult.create("id", "publisher")
        );

        Map<Item, Score> scoreMap = equivalents.candidates();

        assertThat(scoreMap.size(), is(1));
        assertThat(scoreMap.get(item2).asDouble(), is(equalTo(1.0)));
    }

    @Test
    public void testGenerateIsFlexibleAroundStartTimes() {

        final ChannelResolver channelResolver = mock(ChannelResolver.class, "otherChannelResolver");

        when(channelResolver.fromUri(BBC_ONE.getUri())).thenReturn(Maybe.just(BBC_ONE));

        BarbBroadcastMatchingItemEquivalenceGeneratorAndScorer generator =
                BarbBroadcastMatchingItemEquivalenceGeneratorAndScorer.builder()
                        .withScheduleResolver(resolver)
                        .withChannelResolver(channelResolver)
                        .withSupportedPublishers(ImmutableSet.of(BBC))
                        .withScheduleWindow(standardMinutes(10))
                        .withScoreOnMatch(SCORE_ON_MATCH)
                        .withBroadcastFlexibility(standardMinutes(10))
                        .withShortBroadcastFlexibility(standardMinutes(2))
                        .withShortBroadcastMaxDuration(standardMinutes(10))
                        .withTitleMatchingScorer(titleMatchingScorer)
                        .build();

        final Item item1 = episodeWithBroadcasts(
                "subjectItem",
                PA,
                new Broadcast(BBC_ONE.getUri(), time("2014-03-21T15:00:00Z"), time("2014-03-21T15:50:00Z"))
        );
        item1.addCustomField(TXLOG_BROADCASTER_GROUP, "1"); // since we now ignore tier 2 broadcasts

        final Item item2 = episodeWithBroadcasts(
                "equivItem2",
                BBC,
                new Broadcast(BBC_ONE.getUri(), time("2014-03-21T14:50:00Z"), time("2014-03-21T15:50:00Z"))
        );

        final Item item3 = episodeWithBroadcasts(
                "equivItem3",
                BBC,
                new Broadcast(BBC_ONE.getUri(), time("2014-03-21T14:49:00Z"), time("2014-03-21T15:50:00Z"))
        );

        final Item item4 = episodeWithBroadcasts(
                "equivItem4",
                BBC,
                new Broadcast(BBC_ONE.getUri(), time("2014-03-21T15:10:00Z"), time("2014-03-21T15:50:00Z"))
        );

        final Item item5 = episodeWithBroadcasts(
                "equivItem5",
                BBC,
                new Broadcast(BBC_ONE.getUri(), time("2014-03-21T15:11:00Z"), time("2014-03-21T15:50:00Z"))
        );

        setupScheduleResolving("2014-03-21T14:50:00Z", "2014-03-21T16:00:00Z", BBC_ONE, PA, item1);
        setupScheduleResolving("2014-03-21T14:50:00Z", "2014-03-21T16:00:00Z", BBC_ONE, BBC, item2);

        setupTitleScoring(item1, item2, SCORE_ON_MATCH);
        setupTitleScoring(item1, item3, SCORE_ON_MATCH);
        setupTitleScoring(item1, item4, SCORE_ON_MATCH);
        setupTitleScoring(item1, item5, SCORE_ON_MATCH);

        ScoredCandidates<Item> equivalents = generator.generate(
                item1,
                new DefaultDescription(),
                EquivToTelescopeResult.create("id", "publisher")
        );

        Map<Item, Score> scoreMap = equivalents.candidates();

        assertEquals(1, scoreMap.size());
        assertEquals(SCORE_ON_MATCH, scoreMap.get(item2));

        setupScheduleResolving("2014-03-21T14:50:00Z", "2014-03-21T16:00:00Z", BBC_ONE, BBC, item3);

        equivalents = generator.generate(
                item1,
                new DefaultDescription(),
                EquivToTelescopeResult.create("id", "publisher")
        );

        scoreMap = equivalents.candidates();
        assertTrue(scoreMap.isEmpty());

        setupScheduleResolving("2014-03-21T14:50:00Z", "2014-03-21T16:00:00Z", BBC_ONE, BBC, item4);

        equivalents = generator.generate(
                item1,
                new DefaultDescription(),
                EquivToTelescopeResult.create("id", "publisher")
        );

        scoreMap = equivalents.candidates();

        assertEquals(1, scoreMap.size());
        assertEquals(SCORE_ON_MATCH, scoreMap.get(item4));

        setupScheduleResolving("2014-03-21T14:50:00Z", "2014-03-21T16:00:00Z", BBC_ONE, BBC, item5);

        equivalents = generator.generate(
                item1,
                new DefaultDescription(),
                EquivToTelescopeResult.create("id", "publisher")
        );

        scoreMap = equivalents.candidates();
        assertTrue(scoreMap.isEmpty());
    }

    @Test
    public void testGenerateIsFlexibleAroundEndTimes() {

        final ChannelResolver channelResolver = mock(ChannelResolver.class, "otherChannelResolver");

        when(channelResolver.fromUri(BBC_ONE.getUri())).thenReturn(Maybe.just(BBC_ONE));

        BarbBroadcastMatchingItemEquivalenceGeneratorAndScorer generator =
                BarbBroadcastMatchingItemEquivalenceGeneratorAndScorer.builder()
                        .withScheduleResolver(resolver)
                        .withChannelResolver(channelResolver)
                        .withSupportedPublishers(ImmutableSet.of(BBC))
                        .withScheduleWindow(standardMinutes(10))
                        .withScoreOnMatch(SCORE_ON_MATCH)
                        .withBroadcastFlexibility(standardMinutes(10))
                        .withShortBroadcastFlexibility(standardMinutes(2))
                        .withShortBroadcastMaxDuration(standardMinutes(10))
                        .withTitleMatchingScorer(titleMatchingScorer)
                        .build();

        final Item item1 = episodeWithBroadcasts(
                "subjectItem",
                PA,
                new Broadcast(BBC_ONE.getUri(), time("2014-03-21T15:00:00Z"), time("2014-03-21T15:50:00Z"))
        );
        item1.addCustomField(TXLOG_BROADCASTER_GROUP, "1"); // since we now ignore tier 2 broadcasts

        final Item item2 = episodeWithBroadcasts(
                "equivItem2",
                BBC,
                new Broadcast(BBC_ONE.getUri(), time("2014-03-21T15:00:00Z"), time("2014-03-21T16:00:00Z"))
        );

        final Item item3 = episodeWithBroadcasts(
                "equivItem3",
                BBC,
                new Broadcast(BBC_ONE.getUri(), time("2014-03-21T15:00:00Z"), time("2014-03-21T16:01:00Z"))
        );

        final Item item4 = episodeWithBroadcasts(
                "equivItem4",
                BBC,
                new Broadcast(BBC_ONE.getUri(), time("2014-03-21T15:00:00Z"), time("2014-03-21T15:40:00Z"))
        );

        final Item item5 = episodeWithBroadcasts(
                "equivItem5",
                BBC,
                new Broadcast(BBC_ONE.getUri(), time("2014-03-21T15:00:00Z"), time("2014-03-21T15:39:00Z"))
        );

        setupScheduleResolving("2014-03-21T14:50:00Z", "2014-03-21T16:00:00Z", BBC_ONE, PA, item1);
        setupScheduleResolving("2014-03-21T14:50:00Z", "2014-03-21T16:00:00Z", BBC_ONE, BBC, item2);

        setupTitleScoring(item1, item2, SCORE_ON_MATCH);
        setupTitleScoring(item1, item3, SCORE_ON_MATCH);
        setupTitleScoring(item1, item4, SCORE_ON_MATCH);
        setupTitleScoring(item1, item5, SCORE_ON_MATCH);


        ScoredCandidates<Item> equivalents = generator.generate(
                item1,
                new DefaultDescription(),
                EquivToTelescopeResult.create("id", "publisher")
        );

        Map<Item, Score> scoreMap = equivalents.candidates();

        assertEquals(1, scoreMap.size());
        assertEquals(SCORE_ON_MATCH, scoreMap.get(item2));

        setupScheduleResolving("2014-03-21T14:50:00Z", "2014-03-21T16:00:00Z", BBC_ONE, BBC, item3);

        equivalents = generator.generate(
                item1,
                new DefaultDescription(),
                EquivToTelescopeResult.create("id", "publisher")
        );

        scoreMap = equivalents.candidates();
        assertTrue(scoreMap.isEmpty());

        setupScheduleResolving("2014-03-21T14:50:00Z", "2014-03-21T16:00:00Z", BBC_ONE, BBC, item4);

        equivalents = generator.generate(
                item1,
                new DefaultDescription(),
                EquivToTelescopeResult.create("id", "publisher")
        );

        scoreMap = equivalents.candidates();

        assertEquals(1, scoreMap.size());
        assertEquals(SCORE_ON_MATCH, scoreMap.get(item4));

        setupScheduleResolving("2014-03-21T14:50:00Z", "2014-03-21T16:00:00Z", BBC_ONE, BBC, item5);

        equivalents = generator.generate(
                item1,
                new DefaultDescription(),
                EquivToTelescopeResult.create("id", "publisher")
        );

        scoreMap = equivalents.candidates();
        assertTrue(scoreMap.isEmpty());
    }

    @Test
    public void testGenerateHasReducedFlexibilityAroundStartTimesForShortContent() {

        final ChannelResolver channelResolver = mock(ChannelResolver.class, "otherChannelResolver");

        when(channelResolver.fromUri(BBC_ONE.getUri())).thenReturn(Maybe.just(BBC_ONE));

        BarbBroadcastMatchingItemEquivalenceGeneratorAndScorer generator =
                BarbBroadcastMatchingItemEquivalenceGeneratorAndScorer.builder()
                        .withScheduleResolver(resolver)
                        .withChannelResolver(channelResolver)
                        .withSupportedPublishers(ImmutableSet.of(BBC))
                        .withScheduleWindow(standardMinutes(10))
                        .withScoreOnMatch(SCORE_ON_MATCH)
                        .withBroadcastFlexibility(standardMinutes(10))
                        .withShortBroadcastFlexibility(standardMinutes(2))
                        .withShortBroadcastMaxDuration(standardMinutes(10))
                        .withTitleMatchingScorer(titleMatchingScorer)
                        .build();

        final Item item1 = episodeWithBroadcasts(
                "subjectItem",
                PA,
                new Broadcast(BBC_ONE.getUri(), time("2014-03-21T15:00:00Z"), time("2014-03-21T15:05:00Z"))
        );
        item1.addCustomField(TXLOG_BROADCASTER_GROUP, "1"); // since we now ignore tier 2 broadcasts

        final Item item2 = episodeWithBroadcasts(
                "equivItem2",
                BBC,
                new Broadcast(BBC_ONE.getUri(), time("2014-03-21T14:58:00Z"), time("2014-03-21T15:05:00Z"))
        );

        final Item item3 = episodeWithBroadcasts(
                "equivItem3",
                BBC,
                new Broadcast(BBC_ONE.getUri(), time("2014-03-21T14:57:00Z"), time("2014-03-21T15:05:00Z"))
        );

        final Item item4 = episodeWithBroadcasts(
                "equivItem4",
                BBC,
                new Broadcast(BBC_ONE.getUri(), time("2014-03-21T15:02:00Z"), time("2014-03-21T15:05:00Z"))
        );

        final Item item5 = episodeWithBroadcasts(
                "equivItem5",
                BBC,
                new Broadcast(BBC_ONE.getUri(), time("2014-03-21T15:03:00Z"), time("2014-03-21T15:05:00Z"))
        );

        setupScheduleResolving("2014-03-21T14:50:00Z", "2014-03-21T15:15:00Z", BBC_ONE, PA, item1);
        setupScheduleResolving("2014-03-21T14:50:00Z", "2014-03-21T15:15:00Z", BBC_ONE, BBC, item2);

        setupTitleScoring(item1, item2, SCORE_ON_MATCH);
        setupTitleScoring(item1, item3, SCORE_ON_MATCH);
        setupTitleScoring(item1, item4, SCORE_ON_MATCH);
        setupTitleScoring(item1, item5, SCORE_ON_MATCH);


        ScoredCandidates<Item> equivalents = generator.generate(
                item1,
                new DefaultDescription(),
                EquivToTelescopeResult.create("id", "publisher")
        );

        Map<Item, Score> scoreMap = equivalents.candidates();

        assertEquals(1, scoreMap.size());
        assertEquals(SCORE_ON_MATCH, scoreMap.get(item2));

        setupScheduleResolving("2014-03-21T14:50:00Z", "2014-03-21T15:15:00Z", BBC_ONE, BBC, item3);

        equivalents = generator.generate(
                item1,
                new DefaultDescription(),
                EquivToTelescopeResult.create("id", "publisher")
        );

        scoreMap = equivalents.candidates();
        assertTrue(scoreMap.isEmpty());

        setupScheduleResolving("2014-03-21T14:50:00Z", "2014-03-21T15:15:00Z", BBC_ONE, BBC, item4);

        equivalents = generator.generate(
                item1,
                new DefaultDescription(),
                EquivToTelescopeResult.create("id", "publisher")
        );

        scoreMap = equivalents.candidates();

        assertEquals(1, scoreMap.size());
        assertEquals(SCORE_ON_MATCH, scoreMap.get(item4));

        setupScheduleResolving("2014-03-21T14:50:00Z", "2014-03-21T15:15:00Z", BBC_ONE, BBC, item5);

        equivalents = generator.generate(
                item1,
                new DefaultDescription(),
                EquivToTelescopeResult.create("id", "publisher")
        );

        scoreMap = equivalents.candidates();
        assertTrue(scoreMap.isEmpty());
    }

    @Test
    public void testGenerateHasReducedFlexibilityAroundEndTimesForShortContent() {

        final ChannelResolver channelResolver = mock(ChannelResolver.class, "otherChannelResolver");

        when(channelResolver.fromUri(BBC_ONE.getUri())).thenReturn(Maybe.just(BBC_ONE));

        BarbBroadcastMatchingItemEquivalenceGeneratorAndScorer generator =
                BarbBroadcastMatchingItemEquivalenceGeneratorAndScorer.builder()
                        .withScheduleResolver(resolver)
                        .withChannelResolver(channelResolver)
                        .withSupportedPublishers(ImmutableSet.of(BBC))
                        .withScheduleWindow(standardMinutes(10))
                        .withScoreOnMatch(SCORE_ON_MATCH)
                        .withBroadcastFlexibility(standardMinutes(10))
                        .withShortBroadcastFlexibility(standardMinutes(2))
                        .withShortBroadcastMaxDuration(standardMinutes(10))
                        .withTitleMatchingScorer(titleMatchingScorer)
                        .build();

        final Item item1 = episodeWithBroadcasts(
                "subjectItem",
                PA,
                new Broadcast(BBC_ONE.getUri(), time("2014-03-21T15:00:00Z"), time("2014-03-21T15:05:00Z"))
        );
        item1.addCustomField(TXLOG_BROADCASTER_GROUP, "1"); // since we now ignore tier 2 broadcasts

        final Item item2 = episodeWithBroadcasts(
                "equivItem2",
                BBC,
                new Broadcast(BBC_ONE.getUri(), time("2014-03-21T15:00:00Z"), time("2014-03-21T15:03:00Z"))
        );

        final Item item3 = episodeWithBroadcasts(
                "equivItem3",
                BBC,
                new Broadcast(BBC_ONE.getUri(), time("2014-03-21T15:00:00Z"), time("2014-03-21T15:02:00Z"))
        );

        final Item item4 = episodeWithBroadcasts(
                "equivItem4",
                BBC,
                new Broadcast(BBC_ONE.getUri(), time("2014-03-21T15:00:00Z"), time("2014-03-21T15:07:00Z"))
        );

        final Item item5 = episodeWithBroadcasts(
                "equivItem5",
                BBC,
                new Broadcast(BBC_ONE.getUri(), time("2014-03-21T15:00:00Z"), time("2014-03-21T15:08:00Z"))
        );

        setupScheduleResolving("2014-03-21T14:50:00Z", "2014-03-21T15:15:00Z", BBC_ONE, PA, item1);
        setupScheduleResolving("2014-03-21T14:50:00Z", "2014-03-21T15:15:00Z", BBC_ONE, BBC, item2);

        setupTitleScoring(item1, item2, SCORE_ON_MATCH);
        setupTitleScoring(item1, item3, SCORE_ON_MATCH);
        setupTitleScoring(item1, item4, SCORE_ON_MATCH);
        setupTitleScoring(item1, item5, SCORE_ON_MATCH);


        ScoredCandidates<Item> equivalents = generator.generate(
                item1,
                new DefaultDescription(),
                EquivToTelescopeResult.create("id", "publisher")
        );

        Map<Item, Score> scoreMap = equivalents.candidates();

        assertEquals(1, scoreMap.size());
        assertEquals(SCORE_ON_MATCH, scoreMap.get(item2));

        setupScheduleResolving("2014-03-21T14:50:00Z", "2014-03-21T15:15:00Z", BBC_ONE, BBC, item3);

        equivalents = generator.generate(
                item1,
                new DefaultDescription(),
                EquivToTelescopeResult.create("id", "publisher")
        );

        scoreMap = equivalents.candidates();
        assertTrue(scoreMap.isEmpty());

        setupScheduleResolving("2014-03-21T14:50:00Z", "2014-03-21T15:15:00Z", BBC_ONE, BBC, item4);

        equivalents = generator.generate(
                item1,
                new DefaultDescription(),
                EquivToTelescopeResult.create("id", "publisher")
        );

        scoreMap = equivalents.candidates();

        assertEquals(1, scoreMap.size());
        assertEquals(SCORE_ON_MATCH, scoreMap.get(item4));

        setupScheduleResolving("2014-03-21T14:50:00Z", "2014-03-21T15:15:00Z", BBC_ONE, BBC, item5);

        equivalents = generator.generate(
                item1,
                new DefaultDescription(),
                EquivToTelescopeResult.create("id", "publisher")
        );

        scoreMap = equivalents.candidates();
        assertTrue(scoreMap.isEmpty());
    }

    @Test
    public void testGenerateEquivalencesForBbcTwoEnglandTxlogVariants() {
        final Item nitroItem = episodeWithBroadcasts(
                "subjectItem",
                BBC_NITRO,
                new Broadcast(BBC_TWO_ENGLAND.getUri(), time("2014-03-21T15:00:00Z"), time("2014-03-21T16:00:00Z"))
        );

        final Item txlogItem1 = episodeWithBroadcasts(
                "equivItem1",
                BARB_TRANSMISSIONS,
                new Broadcast(BBC_TWO_SOUTH_TXLOG.getUri(), time("2014-03-21T15:00:00Z"), time("2014-03-21T16:00:00Z"))
        );
        txlogItem1.addCustomField(TXLOG_BROADCASTER_GROUP, "1"); // since we now ignore tier 2 broadcasts

        final Item txlogItem2 = episodeWithBroadcasts(
                "equivItem2",
                BARB_TRANSMISSIONS,
                new Broadcast(BBC_TWO_EAST_TXLOG.getUri(), time("2014-03-21T15:00:00Z"), time("2014-03-21T16:00:00Z"))
        );

        setupScheduleResolving("2014-03-21T14:00:00Z", "2014-03-21T17:00:00Z", BBC_TWO_ENGLAND, BBC_NITRO, nitroItem);

        when(
                resolver.unmergedSchedule(
                        time("2014-03-21T14:00:00Z"),
                        time("2014-03-21T17:00:00Z"),
                        ImmutableSet.<Channel>builder()
                                .add(BBC_TWO_ENGLAND)
                                .addAll(BBC_TWO_ENGLAND_TXLOG_CHANNEL_MAP.values())
                                .build(),
                        Sets.difference(PUBLISHERS, ImmutableSet.of(BBC_NITRO))
                )
        ).thenReturn(
                Schedule.fromChannelMap(
                        ImmutableMap.of(
                                BBC_TWO_SOUTH_TXLOG, ImmutableList.of(
                                        fillerScheduleStartEpisode(BARB_TRANSMISSIONS, BBC_TWO_SOUTH_TXLOG),
                                        txlogItem1,
                                        fillerScheduleEndEpisode(BARB_TRANSMISSIONS, BBC_TWO_SOUTH_TXLOG)
                                ),
                                BBC_TWO_EAST_TXLOG, ImmutableList.of(
                                        fillerScheduleStartEpisode(BARB_TRANSMISSIONS, BBC_TWO_EAST_TXLOG),
                                        txlogItem2,
                                        fillerScheduleEndEpisode(BARB_TRANSMISSIONS, BBC_TWO_EAST_TXLOG)
                                )
                        ),
                        interval("2014-03-21T14:00:00Z", "2014-03-21T17:00:00Z")
                )
        );

        setupTitleScoring(nitroItem, txlogItem1, SCORE_ON_MATCH);
        setupTitleScoring(nitroItem, txlogItem2, SCORE_ON_MATCH);
        setupTitleScoring(txlogItem1, nitroItem, SCORE_ON_MATCH);
        setupTitleScoring(txlogItem2, nitroItem, SCORE_ON_MATCH);

        ScoredCandidates<Item> equivalents = generator.generate(
                nitroItem,
                new DefaultDescription(),
                EquivToTelescopeResult.create("id", "publisher")
        );

        Map<Item, Score> scoreMap = equivalents.candidates();

        assertThat(scoreMap.size(), is(2));
        assertThat(scoreMap.get(txlogItem1), is(SCORE_ON_MATCH));
        assertThat(scoreMap.get(txlogItem2), is(SCORE_ON_MATCH));

        setupScheduleResolving("2014-03-21T14:00:00Z", "2014-03-21T17:00:00Z", BBC_TWO_SOUTH_TXLOG, BARB_TRANSMISSIONS, txlogItem1);

        when(
                resolver.unmergedSchedule(
                        time("2014-03-21T14:00:00Z"),
                        time("2014-03-21T17:00:00Z"),
                        ImmutableSet.of(BBC_TWO_ENGLAND, BBC_TWO_SOUTH_TXLOG),
                        Sets.difference(PUBLISHERS, ImmutableSet.of(BARB_TRANSMISSIONS))
                )
        ).thenReturn(
                Schedule.fromChannelMap(
                        ImmutableMap.of(
                                BBC_TWO_ENGLAND, ImmutableList.of(
                                        fillerScheduleStartEpisode(BBC_NITRO, BBC_TWO_ENGLAND),
                                        nitroItem,
                                        fillerScheduleEndEpisode(BBC_NITRO, BBC_TWO_ENGLAND)
                                )
                        ),
                        interval("2014-03-21T14:00:00Z", "2014-03-21T17:00:00Z")
                )
        );

        equivalents = generator.generate(
                txlogItem1,
                new DefaultDescription(),
                EquivToTelescopeResult.create("id", "publisher")
        );

        scoreMap = equivalents.candidates();

        assertThat(scoreMap.size(), is(1));
        assertThat(scoreMap.get(nitroItem), is(SCORE_ON_MATCH));
    }


    @Test
    public void testOffsetScheduleMatchesCorrectEntryInBlock() {
        generator =
                BarbBroadcastMatchingItemEquivalenceGeneratorAndScorer.builder()
                        .withScheduleResolver(resolver)
                        .withChannelResolver(channelResolver)
                        .withSupportedPublishers(PUBLISHERS)
                        .withScheduleWindow(standardHours(3))
                        .withScoreOnMatch(SCORE_ON_MATCH)
                        .withBroadcastFlexibility(standardHours(3))
                        .withShortBroadcastFlexibility(standardMinutes(2))
                        .withShortBroadcastMaxDuration(standardMinutes(10))
                        .withTitleMatchingScorer(titleMatchingScorer)
                        .build();

        final Item previousUnrelatedToSubject = episodeWithBroadcasts(
                "unrelatedItem1",
                Publisher.PA,
                new Broadcast(BBC_ONE.getUri(), time("2014-03-21T13:00:00Z"), time("2014-03-21T14:20:00Z"))
        );

        final Item similarBeforeSubject = episodeWithBroadcasts(
                "similarItem1",
                Publisher.PA,
                new Broadcast(BBC_ONE.getUri(), time("2014-03-21T14:20:00Z"), time("2014-03-21T14:40:00Z"))
        );

        final Item similarBeforeSubject2 = episodeWithBroadcasts(
                "similarItem2",
                Publisher.PA,
                new Broadcast(BBC_ONE.getUri(), time("2014-03-21T14:40:00Z"), time("2014-03-21T15:00:00Z"))
        );

        final Item subject = episodeWithBroadcasts(
                "subjectItem",
                Publisher.PA,
                new Broadcast(BBC_ONE.getUri(), time("2014-03-21T15:00:00Z"), time("2014-03-21T15:20:00Z"))
        );
        subject.addCustomField(TXLOG_BROADCASTER_GROUP, "1"); // since we now ignore tier 2 broadcasts

        final Item similarAfterSubject = episodeWithBroadcasts(
                "similarItem3",
                Publisher.PA,
                new Broadcast(BBC_ONE.getUri(), time("2014-03-21T15:20:00Z"), time("2014-03-21T15:40:00Z"))
        );

        final Item nextUnrelatedToSubject = episodeWithBroadcasts(
                "unrelatedItem2",
                Publisher.PA,
                new Broadcast(BBC_ONE.getUri(), time("2014-03-21T15:40:00Z"), time("2014-03-21T17:00:00Z"))
        );

        List<Item> subjectSchedule = ImmutableList.of(
                previousUnrelatedToSubject,
                similarBeforeSubject,
                similarBeforeSubject2,
                subject,
                similarAfterSubject,
                nextUnrelatedToSubject
        );

        //offset by 1h
        final Item previousUnrelatedToCandidate = episodeWithBroadcasts(
                "unrelatedItem3",
                Publisher.BBC,
                new Broadcast(BBC_ONE.getUri(), time("2014-03-21T14:00:00Z"), time("2014-03-21T15:20:00Z"))
        );

        final Item similarBeforeCandidate = episodeWithBroadcasts(
                "similarItem4",
                Publisher.BBC,
                new Broadcast(BBC_ONE.getUri(), time("2014-03-21T15:20:00Z"), time("2014-03-21T15:40:00Z"))
        );

        final Item similarBeforeCandidate2 = episodeWithBroadcasts(
                "similarItem5",
                Publisher.BBC,
                new Broadcast(BBC_ONE.getUri(), time("2014-03-21T15:40:00Z"), time("2014-03-21T16:00:00Z"))
        );

        final Item candidate = episodeWithBroadcasts(
                "equivItem",
                Publisher.BBC,
                new Broadcast(BBC_ONE.getUri(), time("2014-03-21T16:00:00Z"), time("2014-03-21T16:20:00Z"))
        );

        final Item similarAfterCandidate = episodeWithBroadcasts(
                "similarItem6",
                Publisher.BBC,
                new Broadcast(BBC_ONE.getUri(), time("2014-03-21T16:20:00Z"), time("2014-03-21T16:40:00Z"))
        );

        final Item nextUnrelatedToCandidate = episodeWithBroadcasts(
                "unrelatedItem4",
                Publisher.BBC,
                new Broadcast(BBC_ONE.getUri(), time("2014-03-21T16:40:00Z"), time("2014-03-21T17:00:00Z"))
        );

        final Item similarAfterCandidateInSeparateBlock = episodeWithBroadcasts(
                "similarItem7",
                Publisher.BBC,
                new Broadcast(BBC_ONE.getUri(), time("2014-03-21T17:00:00Z"), time("2014-03-21T17:20:00Z"))
        );

        List<Item> candidateSchedule = ImmutableList.of(
                previousUnrelatedToCandidate,
                similarBeforeCandidate,
                similarBeforeCandidate2,
                candidate,
                similarAfterCandidate,
                nextUnrelatedToCandidate,
                similarAfterCandidateInSeparateBlock
        );

        setupTitleScoring(subject, similarBeforeSubject, Score.ONE);
        setupTitleScoring(subject, similarBeforeSubject2, Score.ONE);
        setupTitleScoring(subject, similarAfterSubject, Score.ONE);
        setupTitleScoring(subject, previousUnrelatedToSubject, Score.nullScore());
        setupTitleScoring(subject, nextUnrelatedToSubject, Score.nullScore());

        setupTitleScoring(subject, candidate, Score.ONE);
        setupTitleScoring(subject, similarBeforeCandidate, Score.ONE);
        setupTitleScoring(subject, similarBeforeCandidate2, Score.ONE);
        setupTitleScoring(subject, similarAfterCandidate, Score.ONE);
        setupTitleScoring(subject, previousUnrelatedToCandidate, Score.nullScore());
        setupTitleScoring(subject, nextUnrelatedToCandidate, Score.nullScore());
        setupTitleScoring(subject, similarAfterCandidateInSeparateBlock, Score.ONE);

        setupTitleScoring(similarBeforeCandidate, similarBeforeCandidate2, Score.ONE);
        setupTitleScoring(similarBeforeCandidate, candidate, Score.ONE);
        setupTitleScoring(similarBeforeCandidate, similarAfterCandidate, Score.ONE);
        setupTitleScoring(similarBeforeCandidate, previousUnrelatedToCandidate, Score.nullScore());
        setupTitleScoring(similarBeforeCandidate, nextUnrelatedToCandidate, Score.nullScore());
        setupTitleScoring(similarBeforeCandidate, similarAfterCandidateInSeparateBlock, Score.ONE);

        when(
                resolver.unmergedSchedule(
                        time("2014-03-21T12:00:00Z"),
                        time("2014-03-21T18:20:00Z"),
                        ImmutableSet.of(BBC_ONE), ImmutableSet.of(PA))
        ).thenReturn(
                Schedule.fromChannelMap(
                        ImmutableMap.of(BBC_ONE, subjectSchedule),
                        interval("2014-03-21T12:00:00Z", "2014-03-21T18:20:00Z")
                )
        );

        when(
                resolver.unmergedSchedule(
                        time("2014-03-21T12:00:00Z"),
                        time("2014-03-21T18:20:00Z"),
                        ImmutableSet.of(BBC_ONE), PUBLISHERS)
        ).thenReturn(
                Schedule.fromChannelMap(
                        ImmutableMap.of(BBC_ONE, candidateSchedule),
                        interval("2014-03-21T12:00:00Z", "2014-03-21T18:20:00Z")
                )
        );

        ScoredCandidates<Item> equivalents = generator.generate(
                subject,
                new DefaultDescription(),
                EquivToTelescopeResult.create("id", "publisher")
        );

        Map<Item, Score> scoreMap = equivalents.candidates();

        assertEquals(1, scoreMap.size());
        Item scored = Iterables.getOnlyElement(scoreMap.keySet());
        assertEquals(candidate, scored);
        assertEquals(SCORE_ON_MATCH, scoreMap.get(scored));
    }

    @Test
    public void testGenerateEquivalencesForTierTwoBroadcasts() {
        final Item item1 = episodeWithBroadcasts("subjectItem", Publisher.PA,
                new Broadcast(BBC_ONE.getUri(), time("2014-03-21T15:00:00Z"), time("2014-03-21T16:00:00Z")),
                new Broadcast(BBC_ONE_CAMBRIDGE.getUri(), time("2014-03-21T15:00:00Z"), time("2014-03-21T16:00:00Z"))
        );

        final Item item2 = episodeWithBroadcasts(
                "equivItem",
                BBC,
                new Broadcast(BBC_ONE.getUri(), time("2014-03-21T15:00:00Z"), time("2014-03-21T16:00:00Z"))
        );

        setupScheduleResolving("2014-03-21T14:00:00Z", "2014-03-21T17:00:00Z", BBC_ONE, PA, item1);
        setupScheduleResolving("2014-03-21T14:00:00Z", "2014-03-21T17:00:00Z", BBC_ONE_CAMBRIDGE, PA, item1);
        setupScheduleResolving("2014-03-21T14:00:00Z", "2014-03-21T17:00:00Z", BBC_ONE, PUBLISHERS, item2);
        setupScheduleResolving("2014-03-21T14:00:00Z", "2014-03-21T17:00:00Z", BBC_ONE_CAMBRIDGE, PUBLISHERS);

        setupTitleScoring(item1, item2, SCORE_ON_MATCH);

        ScoredCandidates<Item> equivalents = generator.generate(
                item1,
                new DefaultDescription(),
                EquivToTelescopeResult.create("id", "publisher")
        );

        Map<Item, Score> scoreMap = equivalents.candidates();

        assertThat(scoreMap.size(), is(0));
    }

    private void setupScheduleResolving(
            String start,
            String end,
            Channel channel,
            Publisher publisher,
            Item... items
    ) {
        setupScheduleResolving(start, end, channel, ImmutableSet.of(publisher), items);
    }

    private void setupScheduleResolving(
            String start,
            String end,
            Channel channel,
            Collection<Publisher> publishers,
            Item... items
    ) {
        Publisher firstPublisher = publishers.iterator().next();
        List<Item> itemList = ImmutableList.<Item>builder()
                .add(fillerScheduleStartEpisode(firstPublisher, channel))
                .addAll(ImmutableList.copyOf(items))
                .add(fillerScheduleEndEpisode(firstPublisher, channel))
                .build();
        when(
                resolver.unmergedSchedule(
                        time(start),
                        time(end),
                        ImmutableSet.of(channel), ImmutableSet.copyOf(publishers))
        ).thenReturn(
                Schedule.fromChannelMap(
                        ImmutableMap.of(channel, itemList),
                        interval(start, end)
                )
        );
    }

    private void setupTitleScoring(Item item1, Item item2, Score score) {
        when(titleMatchingScorer.score(argThat(is(item1)), argThat(is(item2)), any(ResultDescription.class)))
                .thenReturn(score);
    }

    private Interval interval(String startDate, String endDate) {
        return new Interval(time(startDate), time(endDate));
    }

    private DateTime time(String date) {
        return new DateTime(date);
    }

    private Episode episodeWithBroadcasts(String episodeId, Publisher publisher, Broadcast... broadcasts) {
        Episode item = new Episode(episodeId + "Uri", episodeId + "Curie", publisher);
        Version version = new Version();
        version.setCanonicalUri(episodeId + "Version");
        version.setProvider(publisher);
        for (Broadcast broadcast : broadcasts) {
            version.addBroadcast(broadcast);
        }
        item.addVersion(version);
        return item;
    }

    private Episode fillerScheduleStartEpisode(Publisher publisher, Channel channel) {
        return fillerEpisode(publisher, channel, "1111-01-01T01:00:00Z", "1111-01-01T02:00:00Z");
    }

    private Episode fillerScheduleEndEpisode(Publisher publisher, Channel channel) {
        return fillerEpisode(publisher, channel, "3333-01-01T01:00:00Z", "3333-01-01T02:00:00Z");
    }

    private Episode fillerEpisode(Publisher publisher, Channel channel, String start, String end) {
        return episodeWithBroadcasts(
                UUID.randomUUID().toString(),
                publisher,
                new Broadcast(channel.getUri(), time(start), time(end))
        );
    }

}
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

import java.util.List;
import java.util.Map;
import java.util.Set;

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
import static org.mockito.Matchers.any;
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

        generator = new BarbBroadcastMatchingItemEquivalenceGeneratorAndScorer(
                resolver,
                channelResolver,
                PUBLISHERS,
                standardHours(1),
                null,
                SCORE_ON_MATCH,
                titleMatchingScorer
        );
    }

    @Test
    public void testGenerateEquivalencesForOneMatchingBroadcast() {
        final Item item1 = episodeWithBroadcasts("subjectItem", Publisher.PA,
                new Broadcast(BBC_ONE.getUri(), time("2014-03-21T15:00:00Z"), time("2014-03-21T16:00:00Z")),
                new Broadcast(BBC_ONE_CAMBRIDGE.getUri(), time("2014-03-21T15:00:00Z"), time("2014-03-21T16:00:00Z"))
        );

        final Item item2 = episodeWithBroadcasts(
                "equivItem",
                BBC,
                new Broadcast(BBC_ONE.getUri(), time("2014-03-21T15:00:00Z"), time("2014-03-21T16:00:00Z"))
        );

        when(
                resolver.unmergedSchedule(
                        time("2014-03-21T14:00:00Z"),
                        time("2014-03-21T17:00:00Z"),
                        ImmutableSet.of(BBC_ONE), ImmutableSet.of(PA))
        ).thenReturn(
                Schedule.fromChannelMap(
                        ImmutableMap.of(BBC_ONE, ImmutableList.of(item1)),
                        interval("2014-03-21T14:00:00Z", "2014-03-21T17:00:00Z")
                )
        );

        when(
                resolver.unmergedSchedule(
                        time("2014-03-21T14:00:00Z"),
                        time("2014-03-21T17:00:00Z"),
                        ImmutableSet.of(BBC_ONE_CAMBRIDGE), ImmutableSet.of(PA))
        ).thenReturn(
                Schedule.fromChannelMap(
                        ImmutableMap.of(BBC_ONE_CAMBRIDGE, ImmutableList.of(item1)),
                        interval("2014-03-21T14:00:00Z", "2014-03-21T17:00:00Z")
                )
        );

        when(
                resolver.unmergedSchedule(
                        time("2014-03-21T14:00:00Z"),
                        time("2014-03-21T17:00:00Z"),
                        ImmutableSet.of(BBC_ONE),
                        PUBLISHERS)
        ).thenReturn(
                Schedule.fromChannelMap(
                        ImmutableMap.of(BBC_ONE, ImmutableList.of(item2)),
                        interval("2014-03-21T14:00:00Z", "2014-03-21T17:00:00Z")
                )
        );
        when(
                resolver.unmergedSchedule(
                        time("2014-03-21T14:00:00Z"),
                        time("2014-03-21T17:00:00Z"),
                        ImmutableSet.of(BBC_ONE_CAMBRIDGE),
                        PUBLISHERS
                )
        ).thenReturn(
                Schedule.fromChannelMap(
                        ImmutableMap.of(BBC_ONE_CAMBRIDGE, ImmutableList.of()),
                        interval("2014-03-21T14:00:00Z", "2014-03-21T17:00:00Z")
                )
        );

        when(titleMatchingScorer.score(any(Item.class), any(Item.class), any(ResultDescription.class))).thenReturn(Score.ONE);

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
                new BarbBroadcastMatchingItemEquivalenceGeneratorAndScorer(
                        resolver,
                        channelResolver,
                        ImmutableSet.of(BBC),
                        standardMinutes(10),
                        null,
                        SCORE_ON_MATCH,
                        titleMatchingScorer
                );

        final Item item1 = episodeWithBroadcasts(
                "subjectItem",
                PA,
                new Broadcast(BBC_ONE.getUri(), time("2014-03-21T15:00:00Z"), time("2014-03-21T15:50:00Z"))
        );

        final Item item2 = episodeWithBroadcasts(
                "equivItem",
                BBC,
                new Broadcast(BBC_ONE.getUri(), time("2014-03-21T15:00:00Z"), time("2014-03-21T16:00:00Z"))
        );

        when(
                resolver.unmergedSchedule(
                        time("2014-03-21T14:50:00Z"),
                        time("2014-03-21T16:00:00Z"),
                        ImmutableSet.of(BBC_ONE), ImmutableSet.of(PA))
        ).thenReturn(
                Schedule.fromChannelMap(
                        ImmutableMap.of(BBC_ONE, ImmutableList.of(item1)),
                        interval("2014-03-21T14:50:00Z", "2014-03-21T16:00:00Z")
                )
        );

        when(
                resolver.unmergedSchedule(
                        time("2014-03-21T14:50:00Z"),
                        time("2014-03-21T16:00:00Z"),
                        ImmutableSet.of(BBC_ONE), ImmutableSet.of(BBC))
        ).thenReturn(
                Schedule.fromChannelMap(
                        ImmutableMap.of(BBC_ONE, ImmutableList.of(item2)),
                        interval("2014-03-21T14:50:00Z", "2014-03-21T16:00:00Z")
                )
        );

        when(titleMatchingScorer.score(any(Item.class), any(Item.class), any(ResultDescription.class))).thenReturn(Score.ONE);


        ScoredCandidates<Item> equivalents = generator.generate(
                item1,
                new DefaultDescription(),
                EquivToTelescopeResult.create("id", "publisher")
        );

        Map<Item, Score> scoreMap = equivalents.candidates();

        assertThat(scoreMap.size(), is(1));
        assertThat(scoreMap.get(item2).asDouble(), is(equalTo(1.0)));
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
        final Item txlogItem2 = episodeWithBroadcasts(
                "equivItem2",
                BARB_TRANSMISSIONS,
                new Broadcast(BBC_TWO_EAST_TXLOG.getUri(), time("2014-03-21T15:00:00Z"), time("2014-03-21T16:00:00Z"))
        );

        when(
                resolver.unmergedSchedule(
                        time("2014-03-21T14:00:00Z"),
                        time("2014-03-21T17:00:00Z"),
                        ImmutableSet.of(BBC_TWO_ENGLAND), ImmutableSet.of(BBC_NITRO))
        ).thenReturn(
                Schedule.fromChannelMap(
                        ImmutableMap.of(BBC_TWO_ENGLAND, ImmutableList.of(nitroItem)),
                        interval("2014-03-21T14:00:00Z", "2014-03-21T17:00:00Z")
                )
        );

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
                                BBC_TWO_SOUTH_TXLOG, ImmutableList.of(txlogItem1),
                                BBC_TWO_EAST_TXLOG, ImmutableList.of(txlogItem2)
                        ),
                        interval("2014-03-21T14:00:00Z", "2014-03-21T17:00:00Z")
                )
        );

        when(titleMatchingScorer.score(any(Item.class), any(Item.class), any(ResultDescription.class))).thenReturn(Score.ONE);

        ScoredCandidates<Item> equivalents = generator.generate(
                nitroItem,
                new DefaultDescription(),
                EquivToTelescopeResult.create("id", "publisher")
        );

        Map<Item, Score> scoreMap = equivalents.candidates();

        assertThat(scoreMap.size(), is(2));
        assertThat(scoreMap.get(txlogItem1), is(SCORE_ON_MATCH));
        assertThat(scoreMap.get(txlogItem2), is(SCORE_ON_MATCH));

        when(
                resolver.unmergedSchedule(
                        time("2014-03-21T14:00:00Z"),
                        time("2014-03-21T17:00:00Z"),
                        ImmutableSet.of(BBC_TWO_SOUTH_TXLOG), ImmutableSet.of(BARB_TRANSMISSIONS))
        ).thenReturn(
                Schedule.fromChannelMap(
                        ImmutableMap.of(BBC_TWO_SOUTH_TXLOG, ImmutableList.of(txlogItem1)),
                        interval("2014-03-21T14:00:00Z", "2014-03-21T17:00:00Z")
                )
        );

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
                                BBC_TWO_ENGLAND, ImmutableList.of(nitroItem)
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
        generator = new BarbBroadcastMatchingItemEquivalenceGeneratorAndScorer(
                resolver,
                channelResolver,
                PUBLISHERS,
                standardHours(3),
                null,
                SCORE_ON_MATCH,
                titleMatchingScorer
        );

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

        ResultDescription desc = new DefaultDescription();

        when(titleMatchingScorer.score(subject, similarBeforeSubject, desc)).thenReturn(Score.ONE);
        when(titleMatchingScorer.score(subject, similarBeforeSubject2, desc)).thenReturn(Score.ONE);
        when(titleMatchingScorer.score(subject, similarAfterSubject, desc)).thenReturn(Score.ONE);
        when(titleMatchingScorer.score(subject, previousUnrelatedToSubject, desc)).thenReturn(Score.nullScore());
        when(titleMatchingScorer.score(subject, nextUnrelatedToSubject, desc)).thenReturn(Score.nullScore());

        when(titleMatchingScorer.score(subject, candidate, desc)).thenReturn(Score.ONE);
        when(titleMatchingScorer.score(subject, similarBeforeCandidate, desc)).thenReturn(Score.ONE);
        when(titleMatchingScorer.score(subject, similarBeforeCandidate2, desc)).thenReturn(Score.ONE);
        when(titleMatchingScorer.score(subject, similarAfterCandidate, desc)).thenReturn(Score.ONE);
        when(titleMatchingScorer.score(subject, previousUnrelatedToCandidate, desc)).thenReturn(Score.nullScore());
        when(titleMatchingScorer.score(subject, nextUnrelatedToCandidate, desc)).thenReturn(Score.nullScore());
        when(titleMatchingScorer.score(subject, similarAfterCandidateInSeparateBlock, desc)).thenReturn(Score.ONE);

        when(titleMatchingScorer.score(similarBeforeCandidate, similarBeforeCandidate2, desc)).thenReturn(Score.ONE);
        when(titleMatchingScorer.score(similarBeforeCandidate, candidate, desc)).thenReturn(Score.ONE);
        when(titleMatchingScorer.score(similarBeforeCandidate, similarAfterCandidate, desc)).thenReturn(Score.ONE);
        when(titleMatchingScorer.score(similarBeforeCandidate, previousUnrelatedToCandidate, desc)).thenReturn(Score.nullScore());
        when(titleMatchingScorer.score(similarBeforeCandidate, nextUnrelatedToCandidate, desc)).thenReturn(Score.nullScore());
        when(titleMatchingScorer.score(similarBeforeCandidate, similarAfterCandidateInSeparateBlock, desc)).thenReturn(Score.ONE);

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
                desc,
                EquivToTelescopeResult.create("id", "publisher")
        );

        Map<Item, Score> scoreMap = equivalents.candidates();

        assertEquals(1, scoreMap.size());
        Item scored = Iterables.getOnlyElement(scoreMap.keySet());
        assertEquals(candidate, scored);
        assertEquals(SCORE_ON_MATCH, scoreMap.get(scored));
    }

}
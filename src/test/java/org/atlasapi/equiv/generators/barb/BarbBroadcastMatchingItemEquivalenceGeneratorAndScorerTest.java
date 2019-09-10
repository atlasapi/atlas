package org.atlasapi.equiv.generators.barb;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.stream.MoreCollectors;
import com.metabroadcast.common.time.DateTimeZones;
import org.atlasapi.equiv.generators.barb.utils.BarbGeneratorUtils;
import org.atlasapi.equiv.results.description.DefaultDescription;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResult;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.MediaType;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Version;
import org.atlasapi.persistence.content.ScheduleResolver;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatcher;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static org.atlasapi.media.entity.Publisher.BARB_TRANSMISSIONS;
import static org.atlasapi.media.entity.Publisher.BBC;
import static org.atlasapi.media.entity.Publisher.BBC_NITRO;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.joda.time.Duration.standardMinutes;
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

    private final ScheduleResolver resolver = mock(ScheduleResolver.class);
    private final ChannelResolver channelResolver = mock(ChannelResolver.class);
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
                standardMinutes(1),
                null,
                SCORE_ON_MATCH,
                Score.valueOf(0.1),
                null
        );
    }

    @Test
    public void testGenerateEquivalencesForOneMatchingBroadcast() {
        final Item item1 = episodeWithBroadcasts("subjectItem", Publisher.PA,
                new Broadcast(BBC_ONE.getUri(), utcTime(100000), utcTime(2000000)),
                new Broadcast(BBC_ONE_CAMBRIDGE.getUri(), utcTime(100000), utcTime(2000000)));

        final Item item2 = episodeWithBroadcasts(
                "equivItem",
                BBC,
                new Broadcast(BBC_ONE.getUri(), utcTime(100000), utcTime(2000000))
        );

        when(
                resolver.resolveItems(
                        utcTime(40000),
                        utcTime(2060000),
                        ImmutableSet.of(BBC_ONE),
                        PUBLISHERS)
        ).thenReturn(ImmutableSet.of(item2));
        when(
                resolver.resolveItems(
                        utcTime(40000),
                        utcTime(2060000),
                        ImmutableSet.of(BBC_ONE_CAMBRIDGE),
                        PUBLISHERS
                )
        ).thenReturn(ImmutableSet.of());

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
        BarbBroadcastMatchingItemEquivalenceGeneratorAndScorer generator =
                new BarbBroadcastMatchingItemEquivalenceGeneratorAndScorer(
                        resolver,
                        channelResolver,
                        ImmutableSet.of(BBC),
                        standardMinutes(10),
                        null,
                        SCORE_ON_MATCH,
                        Score.valueOf(0.1),
                        null
                );

        final Item item1 = episodeWithBroadcasts(
                "subjectItem",
                Publisher.PA,
                new Broadcast(BBC_ONE.getUri(), time("2014-03-21T15:00:00Z"), time("2014-03-21T15:50:00Z"))
        );

        final Item item2 = episodeWithBroadcasts(
                "equivItem",
                BBC,
                new Broadcast(BBC_ONE.getUri(), time("2014-03-21T15:00:00Z"), time("2014-03-21T16:00:00Z"))
        );

        when(
                resolver.resolveItems(
                        time("2014-03-21T14:50:00Z"),
                        time("2014-03-21T16:00:00Z"),
                        ImmutableSet.of(BBC_ONE), ImmutableSet.of(BBC))
        ).thenReturn(ImmutableSet.of(item2));

        ScoredCandidates<Item> equivalents = generator.generate(
                item1,
                new DefaultDescription(),
                EquivToTelescopeResult.create("id", "publisher")
        );

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

    private Episode episodeWithBroadcasts(
            String episodeId,
            Publisher publisher,
            Channel fillerBroadcastChannel,
            int numberOfFillerBroadcasts,
            Broadcast... broadcasts
    ) {
        Episode item = new Episode(episodeId + "Uri", episodeId + "Curie", publisher);
        Version version = new Version();
        version.setCanonicalUri(episodeId + "Version");
        version.setProvider(publisher);
        for (Broadcast broadcast : broadcasts) {
            version.addBroadcast(broadcast);
        }
        for (int i = 0; i < numberOfFillerBroadcasts; i++) {
            version.addBroadcast(
                    new Broadcast(fillerBroadcastChannel.getUri(), utcTime(i), utcTime(i + 1))
            );
        }
        item.addVersion(version);
        return item;
    }

    @Test
    public void broadcastsWithDurationLessThanTenMinutesHaveLowerFlexibility() {
        Item item1 = episodeWithBroadcasts(
                "subjectitem",
                Publisher.PA,
                new Broadcast(BBC_ONE_CAMBRIDGE.getUri(), utcTime(1000000), utcTime(1200000))
        );

        Item item2 = episodeWithBroadcasts(
                "equivitem",
                BBC,
                new Broadcast(BBC_ONE_CAMBRIDGE.getUri(), utcTime(1000000), utcTime(1200000))
        );

        when(
                resolver.resolveItems(
                        utcTime(880000),
                        utcTime(1320000),
                        ImmutableSet.of(BBC_ONE_CAMBRIDGE),
                        PUBLISHERS)
        ).thenReturn(ImmutableSet.of(item2));

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
    public void testMatchingBroadcastThreshold() {
        final Item item1 = episodeWithBroadcasts(
                "subjectItem",
                BBC_NITRO,
                BBC_ONE_CAMBRIDGE,
                5,
                new Broadcast(BBC_ONE.getUri(), utcTime(100000), utcTime(2000000)),
                new Broadcast(BBC_ONE.getUri(), utcTime(4100000), utcTime(6000000)));

        final Item item2 = episodeWithBroadcasts(
                "equivItem",
                BARB_TRANSMISSIONS,
                BBC_TWO_ENGLAND,
                1,
                new Broadcast(BBC_ONE.getUri(), utcTime(100000), utcTime(2000000)),
                new Broadcast(BBC_ONE.getUri(), utcTime(4100000), utcTime(6000000)));

        Score scoreOnPartialMatch = Score.valueOf(0.1);

        BarbBroadcastMatchingItemEquivalenceGeneratorAndScorer lowerThresholdGenerator =
                new BarbBroadcastMatchingItemEquivalenceGeneratorAndScorer(
                        resolver,
                        channelResolver,
                        ImmutableSet.of(BARB_TRANSMISSIONS),
                        standardMinutes(1),
                        null,
                        SCORE_ON_MATCH,
                        scoreOnPartialMatch,
                        threshold -> threshold > 0.66
                );
        BarbBroadcastMatchingItemEquivalenceGeneratorAndScorer higherThresholdGenerator =
                new BarbBroadcastMatchingItemEquivalenceGeneratorAndScorer(
                        resolver,
                        channelResolver,
                        ImmutableSet.of(BARB_TRANSMISSIONS),
                        standardMinutes(1),
                        null,
                        SCORE_ON_MATCH,
                        scoreOnPartialMatch,
                        threshold -> threshold > 0.7
                );

        when(
                resolver.resolveItems(
                        utcTime(40000),
                        utcTime(2060000),
                        ImmutableSet.of(BBC_ONE),
                        ImmutableSet.of(BARB_TRANSMISSIONS)
                )
        ).thenReturn(ImmutableSet.of(item2));

        when(
                resolver.resolveItems(
                        utcTime(4040000),
                        utcTime(6060000),
                        ImmutableSet.of(BBC_ONE),
                        ImmutableSet.of(BARB_TRANSMISSIONS)
                )
        ).thenReturn(ImmutableSet.of(item2));

        when(
                resolver.resolveItems(
                        any(),
                        any(),
                        argThat(new ArgumentMatcher<Iterable<Channel>>() {
                            @Override
                            public boolean matches(Object argument) {
                                if (argument instanceof Collection<?>) {
                                    return ((Collection<?>) argument).contains(BBC_ONE_CAMBRIDGE);
                                }
                                return false;
                            }
                        }),
                        any()
                )
        ).thenReturn(ImmutableSet.of());

        ScoredCandidates<Item> equivalents = lowerThresholdGenerator.generate(
                item1,
                new DefaultDescription(),
                EquivToTelescopeResult.create("id", "publisher")
        );

        Map<Item, Score> scoreMap = equivalents.candidates();

        assertThat(scoreMap.size(), is(1));
        assertThat(scoreMap.get(item2), is(SCORE_ON_MATCH));

        equivalents = higherThresholdGenerator.generate(
                item1,
                new DefaultDescription(),
                EquivToTelescopeResult.create("id", "publisher")
        );

        scoreMap = equivalents.candidates();

        assertThat(scoreMap.size(), is(1));
        assertThat(scoreMap.get(item2), is(scoreOnPartialMatch));
    }

    @Test
    public void testGenerateEquivalencesForBbcTwoEnglandTxlogVariants() {
        final Item nitroItem = episodeWithBroadcasts(
                "subjectItem",
                BBC_NITRO,
                new Broadcast(BBC_TWO_ENGLAND.getUri(), utcTime(100000), utcTime(2000000))
        );

        final Item txlogItem1 = episodeWithBroadcasts(
                "equivItem1",
                BARB_TRANSMISSIONS,
                new Broadcast(BBC_TWO_SOUTH_TXLOG.getUri(), utcTime(100000), utcTime(2000000))
        );
        final Item txlogItem2 = episodeWithBroadcasts(
                "equivItem2",
                BARB_TRANSMISSIONS,
                new Broadcast(BBC_TWO_EAST_TXLOG.getUri(), utcTime(100000), utcTime(2000000))
        );

        when(
                resolver.resolveItems(
                        utcTime(40000),
                        utcTime(2060000),
                        ImmutableSet.<Channel>builder()
                                .add(BBC_TWO_ENGLAND)
                                .addAll(BBC_TWO_ENGLAND_TXLOG_CHANNEL_MAP.values())
                                .build(),
                        Sets.difference(PUBLISHERS, ImmutableSet.of(BBC_NITRO))
                )
        ).thenReturn(ImmutableSet.of(txlogItem1, txlogItem2));

        when(
                resolver.resolveItems(
                        utcTime(40000),
                        utcTime(2060000),
                        ImmutableSet.of(BBC_TWO_ENGLAND, BBC_TWO_SOUTH_TXLOG),
                        Sets.difference(PUBLISHERS, ImmutableSet.of(BARB_TRANSMISSIONS))
                )
        ).thenReturn(ImmutableSet.of(nitroItem));

        ScoredCandidates<Item> equivalents = generator.generate(
                nitroItem,
                new DefaultDescription(),
                EquivToTelescopeResult.create("id", "publisher")
        );

        Map<Item, Score> scoreMap = equivalents.candidates();

        assertThat(scoreMap.size(), is(2));
        assertThat(scoreMap.get(txlogItem1), is(SCORE_ON_MATCH));
        assertThat(scoreMap.get(txlogItem2), is(SCORE_ON_MATCH));

        equivalents = generator.generate(
                txlogItem1,
                new DefaultDescription(),
                EquivToTelescopeResult.create("id", "publisher")
        );

        scoreMap = equivalents.candidates();

        assertThat(scoreMap.size(), is(1));
        assertThat(scoreMap.get(nitroItem), is(SCORE_ON_MATCH));
    }
}
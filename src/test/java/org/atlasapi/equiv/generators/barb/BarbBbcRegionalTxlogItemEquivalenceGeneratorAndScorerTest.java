package org.atlasapi.equiv.generators.barb;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.stream.MoreCollectors;
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
import org.atlasapi.media.entity.Schedule;
import org.atlasapi.media.entity.Version;
import org.atlasapi.persistence.content.ScheduleResolver;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;
import static junit.framework.TestCase.assertTrue;
import static org.atlasapi.media.entity.Publisher.LAYER3_TXLOGS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BarbBbcRegionalTxlogItemEquivalenceGeneratorAndScorerTest {

    private static final Map<String, Channel> BBC_ONE_CHANNELS = BarbGeneratorUtils.BBC1_TXLOG_CHANNEL_URIS.stream()
            .collect(MoreCollectors.toImmutableMap(
                    uri -> uri,
                    BarbBbcRegionalTxlogItemEquivalenceGeneratorAndScorerTest::createChannel)
            );

    private static final Map<String, Channel> BBC_TWO_CHANNELS = BarbGeneratorUtils.BBC2_TXLOG_CHANNEL_URIS.stream()
            .collect(MoreCollectors.toImmutableMap(
                    uri -> uri,
                    BarbBbcRegionalTxlogItemEquivalenceGeneratorAndScorerTest::createChannel)
            );

    private static final Map<String, Channel> ALL_CHANNELS = ImmutableMap.<String, Channel>builder()
            .putAll(BBC_ONE_CHANNELS)
            .putAll(BBC_TWO_CHANNELS)
            .build();

    private static final Channel BBC_ONE_LONDON =
            checkNotNull(BBC_ONE_CHANNELS.get("http://www.bbc.co.uk/services/bbcone/london"));

    private static final Channel BBC_TWO_SOUTH =
            checkNotNull(BBC_TWO_CHANNELS.get("http://channels.barb.co.uk/channels/1085"));

    private static Channel createChannel(String uri) {
        return Channel.builder()
                .withBroadcaster(Publisher.METABROADCAST)
                .withTitle(uri)
                .withHighDefinition(false)
                .withMediaType(MediaType.VIDEO)
                .withUri(uri)
                .build();
    }

    private static final Score SCORE_ON_MATCH = Score.ONE;

    private static final Duration flexibility = Duration.standardSeconds(10);

    private final ScheduleResolver scheduleResolver = mock(ScheduleResolver.class);
    private BarbBbcRegionalTxlogItemEquivalenceGeneratorAndScorer generator;


    @Before
    public void setUp() {
        final ChannelResolver channelResolver = mock(ChannelResolver.class);

        for (Channel channel : Iterables.concat(BBC_ONE_CHANNELS.values(), BBC_TWO_CHANNELS.values())) {
            when(channelResolver.fromUri(channel.getUri())).thenReturn(Maybe.just(channel));
        }

        generator = BarbBbcRegionalTxlogItemEquivalenceGeneratorAndScorer.builder()
                .withChannelResolver(channelResolver)
                .withScheduleResolver(scheduleResolver)
                .withPublishers(ImmutableSet.of(LAYER3_TXLOGS))
                .withBroadcastFlexibility(flexibility)
                .withScoreOnMatch(SCORE_ON_MATCH)
                .build();
    }

    private void setupScheduleResolving(
            String start,
            String end,
            Duration flexibility,
            Collection<Channel> queriedChannels,
            Publisher publisher,
            List<Item> items
    ) {
        setupScheduleResolving(
                time(start),
                time(end),
                flexibility,
                queriedChannels,
                publisher,
                items
        );
    }

    private void setupScheduleResolving(
            DateTime start,
            DateTime end,
            Duration flexibility,
            Collection<Channel> queriedChannels,
            Publisher publisher,
            List<Item> items
    ) {
        when(
                scheduleResolver.unmergedSchedule(
                        start.minus(flexibility),
                        end.plus(flexibility),
                        queriedChannels, ImmutableSet.of(publisher))
        ).thenReturn(
                Schedule.fromChannelMap(
                        splitByChannel(items),
                        new Interval(start, end)
                )
        );
    }

    private Map<Channel, List<Item>> splitByChannel(Collection<Item> items) {
        return items.stream()
                .collect(
                        MoreCollectors.toImmutableListMultiMap(this::getChannelOfFirstBroadcast, item -> item)
                )
                .asMap()
                .entrySet()
                .stream()
                .collect(
                        MoreCollectors.toImmutableMap(Map.Entry::getKey, entry -> ImmutableList.copyOf(entry.getValue()))
                );
    }

    private DateTime time(String date) {
        return new DateTime(date);
    }

    private Interval interval(String startDate, String endDate) {
        return new Interval(time(startDate), time(endDate));
    }

    private Episode episodeWithBroadcasts(String episodeId, String title, Publisher publisher, Broadcast... broadcasts) {
        Episode item = new Episode(episodeId + "Uri", episodeId + "Curie", publisher);
        item.setTitle(title);
        Version version = new Version();
        version.setCanonicalUri(episodeId + "Version");
        version.setProvider(publisher);
        for (Broadcast broadcast : broadcasts) {
            version.addBroadcast(broadcast);
        }
        item.addVersion(version);
        return item;
    }

    private Channel getChannelOfFirstBroadcast(Item item) {
        return checkNotNull(
                ALL_CHANNELS.get(
                        Iterables.getOnlyElement(
                                Iterables.getOnlyElement(item.getVersions())
                                        .getBroadcasts()
                        ).getBroadcastOn()
                )
        );
    }

    private Set<Channel> bbc1Candidates(Channel channel) {
        return channelCandidates(channel, BBC_ONE_CHANNELS.values());
    }

    private Set<Channel> bbc2Candidates(Channel channel) {
        return channelCandidates(channel, BBC_TWO_CHANNELS.values());
    }

    private Set<Channel> channelCandidates(Channel subject, Collection<Channel> channels) {
        return channels.stream()
                .filter(channel -> subject != channel)
                .collect(MoreCollectors.toImmutableSet());
    }

    @Test
    public void testBbc1RegionalChannelsAreFound() {
        DateTime start = time("2020-01-10T15:05:23Z");
        DateTime end = time("2020-01-10T16:01:19Z");
        String title = "title";

        final Item subject = episodeWithBroadcasts(
                "subjectItem",
                title,
                LAYER3_TXLOGS,
                new Broadcast(BBC_ONE_LONDON.getUri(), start, end)
        );

        List<Item> equivItems = BBC_ONE_CHANNELS.keySet().stream()
                .map(channelUri -> episodeWithBroadcasts(
                        "equivItem-" + channelUri,
                        title,
                        LAYER3_TXLOGS,
                        new Broadcast(channelUri, start, end)
                ))
                .collect(MoreCollectors.toImmutableList());


        setupScheduleResolving(
                start,
                end,
                flexibility,
                bbc1Candidates(getChannelOfFirstBroadcast(subject)),
                LAYER3_TXLOGS,
                equivItems
        );


        ScoredCandidates<Item> equivalents = generator.generate(
                subject,
                new DefaultDescription(),
                EquivToTelescopeResult.create("id", "publisher")
        );

        Map<Item, Score> scoreMap = equivalents.candidates();

        assertThat(scoreMap.size(), is(equivItems.size()));
        for (Map.Entry<Item, Score> entry : scoreMap.entrySet()) {
            assertTrue(equivItems.contains(entry.getKey()));
            assertThat(SCORE_ON_MATCH, is(entry.getValue()));
        }
    }


    @Test
    public void testBbc2RegionalChannelsAreFound() {
        DateTime start = time("2020-01-10T15:05:23Z");
        DateTime end = time("2020-01-10T16:01:19Z");
        String title = "title";

        final Item subjectItem = episodeWithBroadcasts(
                "subjectItem",
                title,
                LAYER3_TXLOGS,
                new Broadcast(BBC_TWO_SOUTH.getUri(), start, end)
        );

        List<Item> equivItems = BBC_TWO_CHANNELS.keySet().stream()
                .map(channelUri -> episodeWithBroadcasts(
                        "equivItem-" + channelUri,
                        title,
                        LAYER3_TXLOGS,
                        new Broadcast(channelUri, start, end)
                ))
                .collect(MoreCollectors.toImmutableList());


        setupScheduleResolving(
                start,
                end,
                flexibility,
                bbc2Candidates(getChannelOfFirstBroadcast(subjectItem)),
                LAYER3_TXLOGS,
                equivItems
        );


        ScoredCandidates<Item> equivalents = generator.generate(
                subjectItem,
                new DefaultDescription(),
                EquivToTelescopeResult.create("id", "publisher")
        );

        Map<Item, Score> scoreMap = equivalents.candidates();

        assertThat(scoreMap.size(), is(equivItems.size()));
        for (Map.Entry<Item, Score> entry : scoreMap.entrySet()) {
            assertTrue(equivItems.contains(entry.getKey()));
            assertThat(SCORE_ON_MATCH, is(entry.getValue()));
        }
    }

    @Test
    public void testDifferentTitlesAreIgnored() {
        DateTime start = time("2020-01-10T15:05:23Z");
        DateTime end = time("2020-01-10T16:01:19Z");
        String title = "title";

        final Item subject = episodeWithBroadcasts(
                "subjectItem",
                title,
                LAYER3_TXLOGS,
                new Broadcast(BBC_ONE_LONDON.getUri(), start, end)
        );

        List<Item> equivItems = BBC_ONE_CHANNELS.keySet().stream()
                .map(channelUri -> episodeWithBroadcasts(
                        "equivItem-" + channelUri,
                        "different" + title,
                        LAYER3_TXLOGS,
                        new Broadcast(channelUri, start, end)
                ))
                .collect(MoreCollectors.toImmutableList());


        setupScheduleResolving(
                start,
                end,
                flexibility,
                bbc1Candidates(getChannelOfFirstBroadcast(subject)),
                LAYER3_TXLOGS,
                equivItems
        );


        ScoredCandidates<Item> equivalents = generator.generate(
                subject,
                new DefaultDescription(),
                EquivToTelescopeResult.create("id", "publisher")
        );

        Map<Item, Score> scoreMap = equivalents.candidates();

        assertThat(scoreMap.size(), is(0));
    }

    @Test
    public void testSimilarTxStartAndEndTimesAreFound() {
        DateTime start = time("2020-01-10T15:05:23Z");
        DateTime end = time("2020-01-10T16:01:19Z");
        String title = "title";

        final Item subject = episodeWithBroadcasts(
                "subjectItem",
                title,
                LAYER3_TXLOGS,
                new Broadcast(BBC_ONE_LONDON.getUri(), start, end)
        );

        List<Item> equivItems = BBC_ONE_CHANNELS.keySet().stream()
                .map(channelUri -> episodeWithBroadcasts(
                        "equivItem-" + channelUri,
                        title,
                        LAYER3_TXLOGS,
                        new Broadcast(channelUri, start.plus(flexibility).minusSeconds(1), end.plus(flexibility).minusSeconds(1))
                ))
                .collect(MoreCollectors.toImmutableList());


        setupScheduleResolving(
                start,
                end,
                flexibility,
                bbc1Candidates(getChannelOfFirstBroadcast(subject)),
                LAYER3_TXLOGS,
                equivItems
        );


        ScoredCandidates<Item> equivalents = generator.generate(
                subject,
                new DefaultDescription(),
                EquivToTelescopeResult.create("id", "publisher")
        );

        Map<Item, Score> scoreMap = equivalents.candidates();

        assertThat(scoreMap.size(), is(equivItems.size()));
        for (Map.Entry<Item, Score> entry : scoreMap.entrySet()) {
            assertTrue(equivItems.contains(entry.getKey()));
            assertThat(SCORE_ON_MATCH, is(entry.getValue()));
        }
    }

    @Test
    public void testDifferentTxStartTimesAreIgnored() {
        DateTime start = time("2020-01-10T15:05:23Z");
        DateTime end = time("2020-01-10T16:01:19Z");
        String title = "title";

        final Item subject = episodeWithBroadcasts(
                "subjectItem",
                title,
                LAYER3_TXLOGS,
                new Broadcast(BBC_ONE_LONDON.getUri(), start, end)
        );

        List<Item> equivItems = BBC_ONE_CHANNELS.keySet().stream()
                .map(channelUri -> episodeWithBroadcasts(
                        "equivItem-" + channelUri,
                        title,
                        LAYER3_TXLOGS,
                        new Broadcast(channelUri, start.plus(flexibility).plusSeconds(1), end)
                ))
                .collect(MoreCollectors.toImmutableList());


        setupScheduleResolving(
                start,
                end,
                flexibility,
                bbc1Candidates(getChannelOfFirstBroadcast(subject)),
                LAYER3_TXLOGS,
                equivItems
        );


        ScoredCandidates<Item> equivalents = generator.generate(
                subject,
                new DefaultDescription(),
                EquivToTelescopeResult.create("id", "publisher")
        );

        Map<Item, Score> scoreMap = equivalents.candidates();

        assertThat(scoreMap.size(), is(0));

        equivItems = BBC_ONE_CHANNELS.keySet().stream()
                .map(channelUri -> episodeWithBroadcasts(
                        "equivItem-" + channelUri,
                        title,
                        LAYER3_TXLOGS,
                        new Broadcast(channelUri, start.minus(flexibility).minusSeconds(1), end)
                ))
                .collect(MoreCollectors.toImmutableList());


        setupScheduleResolving(
                start,
                end,
                flexibility,
                bbc1Candidates(getChannelOfFirstBroadcast(subject)),
                LAYER3_TXLOGS,
                equivItems
        );

        equivalents = generator.generate(
                subject,
                new DefaultDescription(),
                EquivToTelescopeResult.create("id", "publisher")
        );

        scoreMap = equivalents.candidates();

        assertThat(scoreMap.size(), is(0));

    }

    @Test
    public void testDifferentTxEndTimesAreIgnored() {
        DateTime start = time("2020-01-10T15:05:23Z");
        DateTime end = time("2020-01-10T16:01:19Z");
        String title = "title";

        final Item subject = episodeWithBroadcasts(
                "subjectItem",
                title,
                LAYER3_TXLOGS,
                new Broadcast(BBC_ONE_LONDON.getUri(), start, end)
        );

        List<Item> equivItems = BBC_ONE_CHANNELS.keySet().stream()
                .map(channelUri -> episodeWithBroadcasts(
                        "equivItem-" + channelUri,
                        title,
                        LAYER3_TXLOGS,
                        new Broadcast(channelUri, start, end.minus(flexibility).minusSeconds(1))
                ))
                .collect(MoreCollectors.toImmutableList());


        setupScheduleResolving(
                start,
                end,
                flexibility,
                bbc1Candidates(getChannelOfFirstBroadcast(subject)),
                LAYER3_TXLOGS,
                equivItems
        );


        ScoredCandidates<Item> equivalents = generator.generate(
                subject,
                new DefaultDescription(),
                EquivToTelescopeResult.create("id", "publisher")
        );

        Map<Item, Score> scoreMap = equivalents.candidates();

        assertThat(scoreMap.size(), is(0));

        equivItems = BBC_ONE_CHANNELS.keySet().stream()
                .map(channelUri -> episodeWithBroadcasts(
                        "equivItem-" + channelUri,
                        title,
                        LAYER3_TXLOGS,
                        new Broadcast(channelUri, start, end.plus(flexibility).plusSeconds(1))
                ))
                .collect(MoreCollectors.toImmutableList());


        setupScheduleResolving(
                start,
                end,
                flexibility,
                bbc1Candidates(getChannelOfFirstBroadcast(subject)),
                LAYER3_TXLOGS,
                equivItems
        );


        equivalents = generator.generate(
                subject,
                new DefaultDescription(),
                EquivToTelescopeResult.create("id", "publisher")
        );

        scoreMap = equivalents.candidates();

        assertThat(scoreMap.size(), is(0));
    }


}
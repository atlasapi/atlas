package org.atlasapi.equiv.generators.barb;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.time.DateTimeZones;
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

import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.joda.time.Duration.standardHours;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BarbBbcActualTransmissionItemEquivalenceGeneratorAndScorerTest {

    private static final Channel BBC_ONE = Channel.builder()
            .withBroadcaster(Publisher.METABROADCAST)
            .withTitle("BBC One")
            .withHighDefinition(false)
            .withMediaType(MediaType.AUDIO)
            .withUri("http://www.bbc.co.uk/bbcone")
            .build();

    private static final Score SCORE_ON_MATCH = Score.ONE;

    private static final Duration FLEXIBILITY = standardHours(1);

    private final ScheduleResolver resolver = mock(ScheduleResolver.class);
    private BarbBbcActualTransmissionItemEquivalenceGeneratorAndScorer generator;

    @Before
    public void setUp() {
        final ChannelResolver channelResolver = mock(ChannelResolver.class);

        when(channelResolver.fromUri(BBC_ONE.getUri())).thenReturn(Maybe.just(BBC_ONE));

        generator = new BarbBbcActualTransmissionItemEquivalenceGeneratorAndScorer(
                resolver,
                channelResolver,
                FLEXIBILITY,
                broadcast -> true,
                SCORE_ON_MATCH
        );
    }

    @Test
    public void testEquivWhenActualTransmissionIsSame() {
        DateTime publishedStart = DateTime.now().withTime(12, 0, 0, 0);
        DateTime publishedEnd = publishedStart.plusHours(1);
        DateTime actualStart = publishedStart.plusMinutes(5);
        DateTime actualEnd = publishedEnd.plusMinutes(5);
        Broadcast txlogBroadcast = new Broadcast(BBC_ONE.getUri(), actualStart, actualEnd);
        Broadcast nitroBroadcast = new Broadcast(BBC_ONE.getUri(), publishedStart, publishedEnd);
        nitroBroadcast.setActualTransmissionTime(actualStart);
        nitroBroadcast.setActualTransmissionEndTime(actualEnd);

        final Item txlogItem = episodeWithBroadcasts(
                "subjectItem",
                Publisher.BARB_TRANSMISSIONS,
                txlogBroadcast
        );

        final Item nitroItem = episodeWithBroadcasts(
                "equivItem",
                Publisher.BBC_NITRO,
                nitroBroadcast
        );

        setupResolving(txlogItem, nitroItem);

        ScoredCandidates<Item> equivalents;
        Map<Item, Score> scoreMap;

        equivalents = generator.generate(
                txlogItem,
                new DefaultDescription(),
                EquivToTelescopeResult.create("id", "publisher")
        );
        scoreMap = equivalents.candidates();

        assertThat(scoreMap.size(), is(1));
        assertThat(scoreMap.get(nitroItem), is(SCORE_ON_MATCH));

        equivalents = generator.generate(
                nitroItem,
                new DefaultDescription(),
                EquivToTelescopeResult.create("id", "publisher")
        );
        scoreMap = equivalents.candidates();

        assertThat(scoreMap.size(), is(1));
        assertThat(scoreMap.get(txlogItem), is(SCORE_ON_MATCH));
    }

    @Test
    public void testNoEquivWhenActualTransmissionStartIsDifferent() {
        DateTime publishedStart = DateTime.now().withTime(12, 0, 0, 0);
        DateTime publishedEnd = publishedStart.plusHours(1);
        DateTime actualStart = publishedStart.plusMinutes(5);
        DateTime actualEnd = publishedEnd.plusMinutes(5);
        Broadcast txlogBroadcast = new Broadcast(BBC_ONE.getUri(), actualStart, actualEnd);
        Broadcast nitroBroadcast = new Broadcast(BBC_ONE.getUri(), publishedStart, publishedEnd);
        nitroBroadcast.setActualTransmissionTime(actualStart.plusSeconds(2));
        nitroBroadcast.setActualTransmissionEndTime(actualEnd);

        final Item txlogItem = episodeWithBroadcasts(
                "subjectItem",
                Publisher.BARB_TRANSMISSIONS,
                txlogBroadcast
        );

        final Item nitroItem = episodeWithBroadcasts(
                "equivItem",
                Publisher.BBC_NITRO,
                nitroBroadcast
        );

        setupResolving(txlogItem, nitroItem);

        ScoredCandidates<Item> equivalents;
        Map<Item, Score> scoreMap;

        equivalents = generator.generate(
                txlogItem,
                new DefaultDescription(),
                EquivToTelescopeResult.create("id", "publisher")
        );
        scoreMap = equivalents.candidates();

        assertThat(scoreMap.size(), is(0));

        equivalents = generator.generate(
                nitroItem,
                new DefaultDescription(),
                EquivToTelescopeResult.create("id", "publisher")
        );
        scoreMap = equivalents.candidates();

        assertThat(scoreMap.size(), is(0));
    }

    @Test
    public void testNoEquivWhenActualTransmissionEndIsDifferent() {
        DateTime publishedStart = DateTime.now().withTime(12, 0, 0, 0);
        DateTime publishedEnd = publishedStart.plusHours(1);
        DateTime actualStart = publishedStart.plusMinutes(5);
        DateTime actualEnd = publishedEnd.plusMinutes(5);
        Broadcast txlogBroadcast = new Broadcast(BBC_ONE.getUri(), actualStart, actualEnd);
        Broadcast nitroBroadcast = new Broadcast(BBC_ONE.getUri(), publishedStart, publishedEnd);
        nitroBroadcast.setActualTransmissionTime(actualStart);
        nitroBroadcast.setActualTransmissionEndTime(actualEnd.plusSeconds(2));

        final Item txlogItem = episodeWithBroadcasts(
                "subjectItem",
                Publisher.BARB_TRANSMISSIONS,
                txlogBroadcast
        );

        final Item nitroItem = episodeWithBroadcasts(
                "equivItem",
                Publisher.BBC_NITRO,
                nitroBroadcast
        );

        setupResolving(txlogItem, nitroItem);

        ScoredCandidates<Item> equivalents;
        Map<Item, Score> scoreMap;

        equivalents = generator.generate(
                txlogItem,
                new DefaultDescription(),
                EquivToTelescopeResult.create("id", "publisher")
        );
        scoreMap = equivalents.candidates();

        assertThat(scoreMap.size(), is(0));

        equivalents = generator.generate(
                nitroItem,
                new DefaultDescription(),
                EquivToTelescopeResult.create("id", "publisher")
        );
        scoreMap = equivalents.candidates();

        assertThat(scoreMap.size(), is(0));
    }

    @Test
    public void testEquivWhenActualTransmissionWithinASecond1() {
        DateTime publishedStart = DateTime.now().withTime(12, 0, 0, 0);
        DateTime publishedEnd = publishedStart.plusHours(1);
        DateTime actualStart = publishedStart.plusMinutes(5);
        DateTime actualEnd = publishedEnd.plusMinutes(5);
        Broadcast txlogBroadcast = new Broadcast(BBC_ONE.getUri(), actualStart, actualEnd);
        Broadcast nitroBroadcast = new Broadcast(BBC_ONE.getUri(), publishedStart, publishedEnd);
        nitroBroadcast.setActualTransmissionTime(actualStart.minusSeconds(1));
        nitroBroadcast.setActualTransmissionEndTime(actualEnd.plusSeconds(1));

        final Item txlogItem = episodeWithBroadcasts(
                "subjectItem",
                Publisher.BARB_TRANSMISSIONS,
                txlogBroadcast
        );

        final Item nitroItem = episodeWithBroadcasts(
                "equivItem",
                Publisher.BBC_NITRO,
                nitroBroadcast
        );

        setupResolving(txlogItem, nitroItem);

        ScoredCandidates<Item> equivalents;
        Map<Item, Score> scoreMap;

        equivalents = generator.generate(
                txlogItem,
                new DefaultDescription(),
                EquivToTelescopeResult.create("id", "publisher")
        );
        scoreMap = equivalents.candidates();

        assertThat(scoreMap.size(), is(1));
        assertThat(scoreMap.get(nitroItem), is(SCORE_ON_MATCH));

        equivalents = generator.generate(
                nitroItem,
                new DefaultDescription(),
                EquivToTelescopeResult.create("id", "publisher")
        );
        scoreMap = equivalents.candidates();

        assertThat(scoreMap.size(), is(1));
        assertThat(scoreMap.get(txlogItem), is(SCORE_ON_MATCH));
    }

    @Test
    public void testEquivWhenActualTransmissionWithinASecond2() {
        DateTime publishedStart = DateTime.now().withTime(12, 0, 0, 0);
        DateTime publishedEnd = publishedStart.plusHours(1);
        DateTime actualStart = publishedStart.plusMinutes(5);
        DateTime actualEnd = publishedEnd.plusMinutes(5);
        Broadcast txlogBroadcast = new Broadcast(BBC_ONE.getUri(), actualStart, actualEnd);
        Broadcast nitroBroadcast = new Broadcast(BBC_ONE.getUri(), publishedStart, publishedEnd);
        nitroBroadcast.setActualTransmissionTime(actualStart.plusSeconds(1));
        nitroBroadcast.setActualTransmissionEndTime(actualEnd.minusSeconds(1));

        final Item txlogItem = episodeWithBroadcasts(
                "subjectItem",
                Publisher.BARB_TRANSMISSIONS,
                txlogBroadcast
        );

        final Item nitroItem = episodeWithBroadcasts(
                "equivItem",
                Publisher.BBC_NITRO,
                nitroBroadcast
        );

        setupResolving(txlogItem, nitroItem);

        ScoredCandidates<Item> equivalents;
        Map<Item, Score> scoreMap;

        equivalents = generator.generate(
                txlogItem,
                new DefaultDescription(),
                EquivToTelescopeResult.create("id", "publisher")
        );
        scoreMap = equivalents.candidates();

        assertThat(scoreMap.size(), is(1));
        assertThat(scoreMap.get(nitroItem), is(SCORE_ON_MATCH));

        equivalents = generator.generate(
                nitroItem,
                new DefaultDescription(),
                EquivToTelescopeResult.create("id", "publisher")
        );
        scoreMap = equivalents.candidates();

        assertThat(scoreMap.size(), is(1));
        assertThat(scoreMap.get(txlogItem), is(SCORE_ON_MATCH));
    }

    @Test
    public void testNoEquivWhenActualTransmissionStartIsNull() {
        DateTime publishedStart = DateTime.now().withTime(12, 0, 0, 0);
        DateTime publishedEnd = publishedStart.plusHours(1);
        DateTime actualStart = publishedStart.plusMinutes(5);
        DateTime actualEnd = publishedEnd.plusMinutes(5);
        Broadcast txlogBroadcast = new Broadcast(BBC_ONE.getUri(), actualStart, actualEnd);
        Broadcast nitroBroadcast = new Broadcast(BBC_ONE.getUri(), publishedStart, publishedEnd);
        nitroBroadcast.setActualTransmissionEndTime(actualEnd);

        final Item txlogItem = episodeWithBroadcasts(
                "subjectItem",
                Publisher.BARB_TRANSMISSIONS,
                txlogBroadcast
        );

        final Item nitroItem = episodeWithBroadcasts(
                "equivItem",
                Publisher.BBC_NITRO,
                nitroBroadcast
        );

        setupResolving(txlogItem, nitroItem);

        ScoredCandidates<Item> equivalents;
        Map<Item, Score> scoreMap;

        equivalents = generator.generate(
                txlogItem,
                new DefaultDescription(),
                EquivToTelescopeResult.create("id", "publisher")
        );
        scoreMap = equivalents.candidates();

        assertThat(scoreMap.size(), is(0));

        equivalents = generator.generate(
                nitroItem,
                new DefaultDescription(),
                EquivToTelescopeResult.create("id", "publisher")
        );
        scoreMap = equivalents.candidates();

        assertThat(scoreMap.size(), is(0));
    }

    @Test
    public void testNoEquivWhenActualTransmissionEndIsNull() {
        DateTime publishedStart = DateTime.now().withTime(12, 0, 0, 0);
        DateTime publishedEnd = publishedStart.plusHours(1);
        DateTime actualStart = publishedStart.plusMinutes(5);
        DateTime actualEnd = publishedEnd.plusMinutes(5);
        Broadcast txlogBroadcast = new Broadcast(BBC_ONE.getUri(), actualStart, actualEnd);
        Broadcast nitroBroadcast = new Broadcast(BBC_ONE.getUri(), publishedStart, publishedEnd);
        nitroBroadcast.setActualTransmissionTime(actualStart);

        final Item txlogItem = episodeWithBroadcasts(
                "subjectItem",
                Publisher.BARB_TRANSMISSIONS,
                txlogBroadcast
        );

        final Item nitroItem = episodeWithBroadcasts(
                "equivItem",
                Publisher.BBC_NITRO,
                nitroBroadcast
        );

        setupResolving(txlogItem, nitroItem);

        ScoredCandidates<Item> equivalents;
        Map<Item, Score> scoreMap;

        equivalents = generator.generate(
                txlogItem,
                new DefaultDescription(),
                EquivToTelescopeResult.create("id", "publisher")
        );
        scoreMap = equivalents.candidates();

        assertThat(scoreMap.size(), is(0));

        equivalents = generator.generate(
                nitroItem,
                new DefaultDescription(),
                EquivToTelescopeResult.create("id", "publisher")
        );
        scoreMap = equivalents.candidates();

        assertThat(scoreMap.size(), is(0));
    }

    @Test
    public void testNoEquivWhenActualTransmissionOutsideFlexibilityWindow() {
        DateTime publishedStart = DateTime.now().withTime(12, 0, 0, 0);
        DateTime publishedEnd = publishedStart.plusHours(1);
        DateTime actualStart = publishedStart.plus(FLEXIBILITY).plusMinutes(5);
        DateTime actualEnd = publishedEnd.plus(FLEXIBILITY).plusMinutes(5);
        Broadcast txlogBroadcast = new Broadcast(BBC_ONE.getUri(), actualStart, actualEnd);
        Broadcast nitroBroadcast = new Broadcast(BBC_ONE.getUri(), publishedStart, publishedEnd);
        nitroBroadcast.setActualTransmissionTime(actualStart);
        nitroBroadcast.setActualTransmissionEndTime(actualEnd);

        final Item txlogItem = episodeWithBroadcasts(
                "subjectItem",
                Publisher.BARB_TRANSMISSIONS,
                txlogBroadcast
        );

        final Item nitroItem = episodeWithBroadcasts(
                "equivItem",
                Publisher.BBC_NITRO,
                nitroBroadcast
        );

        setupResolving(txlogItem, nitroItem);

        ScoredCandidates<Item> equivalents;
        Map<Item, Score> scoreMap;

        equivalents = generator.generate(
                txlogItem,
                new DefaultDescription(),
                EquivToTelescopeResult.create("id", "publisher")
        );
        scoreMap = equivalents.candidates();

        assertThat(scoreMap.size(), is(0));

        equivalents = generator.generate(
                nitroItem,
                new DefaultDescription(),
                EquivToTelescopeResult.create("id", "publisher")
        );
        scoreMap = equivalents.candidates();

        assertThat(scoreMap.size(), is(0));
    }

    @Test
    public void testEquivOnRealExample() {
        // Nitro:
        // <published_time start="2019-06-17T18:00:00Z" end="2019-06-17T18:30:00Z" duration="PT30M"/>
        // <tx_time start="2019-06-17T17:58:00.2Z" end="2019-06-17T18:27:23.72Z"/>
        // Txlog: start: 185800 end: 192723 (Local time)
        DateTime txlogStart = DateTime.parse("2019-06-17T17:58:00Z");
        DateTime txlogEnd = DateTime.parse("2019-06-17T18:27:23Z");
        DateTime nitroPublishedStart = DateTime.parse("2019-06-17T18:00:00Z");
        DateTime nitroPublishedEnd = DateTime.parse("2019-06-17T18:30:00Z");
        DateTime nitroActualStart = DateTime.parse("2019-06-17T17:58:00.2Z");
        DateTime nitroActualEnd = DateTime.parse("2019-06-17T18:27:23.72Z");

        Broadcast txlogBroadcast = new Broadcast(BBC_ONE.getUri(), txlogStart, txlogEnd);
        Broadcast nitroBroadcast = new Broadcast(BBC_ONE.getUri(), nitroPublishedStart, nitroPublishedEnd);
        nitroBroadcast.setActualTransmissionTime(nitroActualStart);
        nitroBroadcast.setActualTransmissionEndTime(nitroActualEnd);

        final Item txlogItem = episodeWithBroadcasts(
                "subjectItem",
                Publisher.BARB_TRANSMISSIONS,
                txlogBroadcast
        );

        final Item nitroItem = episodeWithBroadcasts(
                "equivItem",
                Publisher.BBC_NITRO,
                nitroBroadcast
        );

        setupResolving(txlogItem, nitroItem);

        ScoredCandidates<Item> equivalents;
        Map<Item, Score> scoreMap;

        equivalents = generator.generate(
                txlogItem,
                new DefaultDescription(),
                EquivToTelescopeResult.create("id", "publisher")
        );
        scoreMap = equivalents.candidates();

        assertThat(scoreMap.size(), is(1));
        assertThat(scoreMap.get(nitroItem), is(SCORE_ON_MATCH));

        equivalents = generator.generate(
                nitroItem,
                new DefaultDescription(),
                EquivToTelescopeResult.create("id", "publisher")
        );
        scoreMap = equivalents.candidates();

        assertThat(scoreMap.size(), is(1));
        assertThat(scoreMap.get(txlogItem), is(SCORE_ON_MATCH));
    }

    private void setupResolving(Item txlogItem, Item nitroItem) {
        Broadcast txlogBroadcast =
                Iterables.getOnlyElement(Iterables.getOnlyElement(txlogItem.getVersions()).getBroadcasts());
        DateTime queryStart = txlogBroadcast.getTransmissionTime().minus(FLEXIBILITY);
        DateTime queryEnd = txlogBroadcast.getTransmissionEndTime().plus(FLEXIBILITY);

        when(resolver.unmergedSchedule(
                queryStart,
                queryEnd,
                ImmutableSet.of(BBC_ONE),
                ImmutableSet.of(nitroItem.getPublisher())
        ))
                .thenReturn(Schedule.fromChannelMap(
                        ImmutableMap.of(BBC_ONE,
                                ImmutableList.of(nitroItem)),
                        interval(queryStart, queryEnd)
                ));

        Broadcast nitroBroadcast =
                Iterables.getOnlyElement(Iterables.getOnlyElement(nitroItem.getVersions()).getBroadcasts());
        queryStart = nitroBroadcast.getTransmissionTime().minus(FLEXIBILITY);
        queryEnd = nitroBroadcast.getTransmissionEndTime().plus(FLEXIBILITY);

        when(resolver.unmergedSchedule(
                queryStart,
                queryEnd,
                ImmutableSet.of(BBC_ONE),
                ImmutableSet.of(txlogItem.getPublisher())
        ))
                .thenReturn(Schedule.fromChannelMap(
                        ImmutableMap.of(BBC_ONE,
                                ImmutableList.of(txlogItem)),
                        interval(queryStart, queryEnd)
                ));

    }

    private Interval interval(DateTime start, DateTime end) {
        return new Interval(start.getMillis(), end.getMillis(), DateTimeZones.UTC);
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

}
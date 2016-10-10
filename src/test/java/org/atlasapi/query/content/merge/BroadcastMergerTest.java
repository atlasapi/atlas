package org.atlasapi.query.content.merge;

import org.atlasapi.media.entity.Broadcast;

import com.google.common.collect.ImmutableSet;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class BroadcastMergerTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void missingAssertionValueFailsParsing() throws Exception {
        exception.expect(IllegalArgumentException.class);
        BroadcastMerger.parse("\"cbkM\"|\"2016-01-01T00:00:00Z\"");
    }

    @Test
    public void fromDateAfterToFailsParsing() throws Exception {
        exception.expect(IllegalArgumentException.class);
        BroadcastMerger.parse("\"cbkM\"|\"2016-01-02T00:00:00Z\"|\"2016-01-01T00:00:00Z\"");
    }

    @Test
    public void fromDateSameAsToFailsParsing() throws Exception {
        exception.expect(IllegalArgumentException.class);
        BroadcastMerger.parse("\"cbkM\"|\"2016-01-01T00:00:00Z\"|\"2016-01-01T00:00:00Z\"");
    }

    @Test
    public void invalidDateFailsParsing() throws Exception {
        exception.expect(IllegalArgumentException.class);
        BroadcastMerger.parse("\"cbkM\"|\"2016-01-01T00:00:00Z\"|\"2016-01-01T99:00:00Z\"");
    }

    @Test
    public void mergerWithMissingQuotesFailsParsing() throws Exception {
        exception.expect(IllegalArgumentException.class);
        BroadcastMerger.parse("\"cbkM\"|\"2016-01-01T00:00:00Z|\"2016-01-01T12:00:00Z\"");
    }

    @Test
    public void mergerWithLeadingAndTrailingWhitespaceDoesNotFailParsing() throws Exception {
        BroadcastMerger.parse(" \"cbkM\"|\"2016-01-01T00:00:00Z\"|\"2016-01-01T12:00:00Z\" ");
    }

    @Test
    public void nullAssertionParameterRemovesAllExistingBroadcasts() throws Exception {
        BroadcastMerger merger = BroadcastMerger.parse(null);

        Broadcast broadcast = new Broadcast("channelUri", DateTime.now(), DateTime.now());

        ImmutableSet<Broadcast> merge = merger.merge(
                ImmutableSet.of(),
                ImmutableSet.of(broadcast),
                true
        );

        assertThat(merge.isEmpty(), is(true));
    }

    @Test
    public void emptyAssertionParameterRemovesAllExistingBroadcasts() throws Exception {
        BroadcastMerger merger = BroadcastMerger.parse("");

        Broadcast broadcast = new Broadcast("channelUri", DateTime.now(), DateTime.now());

        ImmutableSet<Broadcast> merge = merger.merge(
                ImmutableSet.of(),
                ImmutableSet.of(broadcast),
                true
        );

        assertThat(merge.isEmpty(), is(true));
    }

    @Test
    public void nullAssertionParameterConsidersAllUpdateBroadcastsValid() throws Exception {
        BroadcastMerger merger = BroadcastMerger.parse(null);

        Broadcast broadcast = new Broadcast("channelUri", DateTime.now(), DateTime.now());

        ImmutableSet<Broadcast> merge = merger.merge(
                ImmutableSet.of(broadcast),
                ImmutableSet.of(),
                true
        );

        assertThat(merge.contains(broadcast), is(true));
    }

    @Test
    public void emptyAssertionParameterConsidersAllUpdateBroadcastsValid() throws Exception {
        BroadcastMerger merger = BroadcastMerger.parse("");

        Broadcast broadcast = new Broadcast("channelUri", DateTime.now(), DateTime.now());

        ImmutableSet<Broadcast> merge = merger.merge(
                ImmutableSet.of(broadcast),
                ImmutableSet.of(),
                true
        );

        assertThat(merge.contains(broadcast), is(true));
    }

    @Test
    public void mergerPreservesNonOverlappingBroadcast() throws Exception {
        BroadcastMerger merger = BroadcastMerger.parse(
                "\"channelUri\"|\"2016-01-01T00:00:00Z\"|\"2016-01-02T00:00:00Z\""
        );

        Broadcast broadcast = new Broadcast(
                "channelUri",
                new DateTime(2016, 2, 1, 0, 0, 0, DateTimeZone.UTC),
                new DateTime(2016, 2, 2, 0, 0, 0, DateTimeZone.UTC)
        );

        ImmutableSet<Broadcast> merge = merger.merge(
                ImmutableSet.of(),
                ImmutableSet.of(broadcast),
                true
        );

        assertThat(merge.contains(broadcast), is(true));
    }

    @Test
    public void mergerPreservesBroadcastThatEndsWhenTheAssertionBegins() throws Exception {
        BroadcastMerger merger = BroadcastMerger.parse(
                "\"channelUri\"|\"2016-01-05T00:00:00Z\"|\"2016-01-06T00:00:00Z\""
        );

        Broadcast broadcast = new Broadcast(
                "channelUri",
                new DateTime(2016, 1, 4, 0, 0, 0, DateTimeZone.UTC),
                new DateTime(2016, 1, 5, 0, 0, 0, DateTimeZone.UTC)
        );

        ImmutableSet<Broadcast> merge = merger.merge(
                ImmutableSet.of(),
                ImmutableSet.of(broadcast),
                true
        );

        assertThat(merge.contains(broadcast), is(true));
    }

    @Test
    public void mergerPreservesBroadcastThatBeginsWhenTheAssertionEnds() throws Exception {
        BroadcastMerger merger = BroadcastMerger.parse(
                "\"channelUri\"|\"2016-01-05T00:00:00Z\"|\"2016-01-06T00:00:00Z\""
        );

        Broadcast broadcast = new Broadcast(
                "channelUri",
                new DateTime(2016, 1, 6, 0, 0, 0, DateTimeZone.UTC),
                new DateTime(2016, 1, 7, 0, 0, 0, DateTimeZone.UTC)
        );

        ImmutableSet<Broadcast> merge = merger.merge(
                ImmutableSet.of(),
                ImmutableSet.of(broadcast),
                true
        );

        assertThat(merge.contains(broadcast), is(true));
    }

    @Test
    public void mergerPreservesBroadcastOnDifferentChannel() throws Exception {
        BroadcastMerger merger = BroadcastMerger.parse(
                "\"channelUri\"|\"2016-01-05T00:00:00Z\"|\"2016-01-06T00:00:00Z\""
        );

        Broadcast broadcast = new Broadcast(
                "otherChannelUri",
                new DateTime(2016, 1, 5, 0, 0, 0, DateTimeZone.UTC),
                new DateTime(2016, 1, 6, 0, 0, 0, DateTimeZone.UTC)
        );

        ImmutableSet<Broadcast> merge = merger.merge(
                ImmutableSet.of(),
                ImmutableSet.of(broadcast),
                true
        );

        assertThat(merge.contains(broadcast), is(true));
    }

    @Test
    public void mergerDoesNotPreserveFullyOverlappingBroadcast() throws Exception {
        BroadcastMerger merger = BroadcastMerger.parse(
                "\"channelUri\"|\"2016-01-05T00:00:00Z\"|\"2016-01-06T00:00:00Z\""
        );

        Broadcast broadcast = new Broadcast(
                "channelUri",
                new DateTime(2016, 1, 5, 12, 0, 0, DateTimeZone.UTC),
                new DateTime(2016, 1, 6, 13, 0, 0, DateTimeZone.UTC)
        );

        ImmutableSet<Broadcast> merge = merger.merge(
                ImmutableSet.of(),
                ImmutableSet.of(broadcast),
                true
        );

        assertThat(merge.isEmpty(), is(true));
    }

    @Test
    public void mergerDoesNotPreserveOverlappingAtStartBroadcast() throws Exception {
        BroadcastMerger merger = BroadcastMerger.parse(
                "\"channelUri\"|\"2016-01-05T00:00:00Z\"|\"2016-01-06T00:00:00Z\""
        );

        Broadcast broadcast = new Broadcast(
                "channelUri",
                new DateTime(2016, 1, 4, 22, 0, 0, DateTimeZone.UTC),
                new DateTime(2016, 1, 5, 2, 0, 0, DateTimeZone.UTC)
        );

        ImmutableSet<Broadcast> merge = merger.merge(
                ImmutableSet.of(),
                ImmutableSet.of(broadcast),
                true
        );

        assertThat(merge.isEmpty(), is(true));
    }

    @Test
    public void mergerDoesNotPreserveOverlappingAtEndBroadcast() throws Exception {
        BroadcastMerger merger = BroadcastMerger.parse(
                "\"channelUri\"|\"2016-01-05T00:00:00Z\"|\"2016-01-06T00:00:00Z\""
        );

        Broadcast broadcast = new Broadcast(
                "channelUri",
                new DateTime(2016, 1, 5, 22, 0, 0, DateTimeZone.UTC),
                new DateTime(2016, 1, 6, 2, 0, 0, DateTimeZone.UTC)
        );

        ImmutableSet<Broadcast> merge = merger.merge(
                ImmutableSet.of(),
                ImmutableSet.of(broadcast),
                true
        );

        assertThat(merge.isEmpty(), is(true));
    }

    @Test
    public void MergerWithMultipleAssertionsDoesNotPreserveOverlappingBroadcasts()
            throws Exception {
        BroadcastMerger merger = BroadcastMerger.parse(""
                + "\"channelUri\"|\"2016-01-05T00:00:00Z\"|\"2016-01-06T00:00:00Z\","
                + "\"channelUri\"|\"2016-02-06T12:00:00Z\"|\"2016-02-06T14:00:00Z\","
                + "\"otherChannelUri\"|\"2016-02-06T12:00:00Z\"|\"2016-02-06T14:00:00Z\""
        );

        Broadcast firstBroadcast = new Broadcast(
                "channelUri",
                new DateTime(2016, 1, 5, 10, 0, 0, DateTimeZone.UTC),
                new DateTime(2016, 1, 5, 12, 0, 0, DateTimeZone.UTC)
        );
        Broadcast secondBroadcast = new Broadcast(
                "channelUri",
                new DateTime(2016, 2, 6, 12, 0, 0, DateTimeZone.UTC),
                new DateTime(2016, 2, 6, 14, 0, 0, DateTimeZone.UTC)
        );
        Broadcast thirdBroadcast = new Broadcast(
                "otherChannelUri",
                new DateTime(2016, 2, 6, 12, 0, 0, DateTimeZone.UTC),
                new DateTime(2016, 2, 6, 14, 0, 0, DateTimeZone.UTC)
        );

        ImmutableSet<Broadcast> merge = merger.merge(
                ImmutableSet.of(),
                ImmutableSet.of(firstBroadcast, secondBroadcast, thirdBroadcast),
                true
        );

        assertThat(merge.isEmpty(), is(true));
    }

    @Test
    public void mergerKeepsOverlappingUpdateBroadcast() throws Exception {
        BroadcastMerger merger = BroadcastMerger.parse(
                "\"channelUri\"|\"2016-01-05T00:00:00Z\"|\"2016-01-06T00:00:00Z\""
        );

        Broadcast broadcast = new Broadcast(
                "channelUri",
                new DateTime(2016, 1, 5, 22, 0, 0, DateTimeZone.UTC),
                new DateTime(2016, 1, 5, 23, 0, 0, DateTimeZone.UTC)
        );

        ImmutableSet<Broadcast> merge = merger.merge(
                ImmutableSet.of(broadcast),
                ImmutableSet.of(),
                true
        );

        assertThat(merge.contains(broadcast), is(true));
    }

    @Test
    public void mergerKeepsOverlappingUpdateBroadcastAtStartOfInterval() throws Exception {
        BroadcastMerger merger = BroadcastMerger.parse(
                "\"channelUri\"|\"2016-01-05T00:00:00Z\"|\"2016-01-06T00:00:00Z\""
        );

        Broadcast broadcast = new Broadcast(
                "channelUri",
                new DateTime(2016, 1, 5, 0, 0, 0, DateTimeZone.UTC),
                new DateTime(2016, 1, 5, 2, 0, 0, DateTimeZone.UTC)
        );

        ImmutableSet<Broadcast> merge = merger.merge(
                ImmutableSet.of(broadcast),
                ImmutableSet.of(),
                true
        );

        assertThat(merge.contains(broadcast), is(true));
    }

    @Test
    public void mergerKeepsOverlappingUpdateBroadcastAtEndOfInterval() throws Exception {
        BroadcastMerger merger = BroadcastMerger.parse(
                "\"channelUri\"|\"2016-01-05T00:00:00Z\"|\"2016-01-06T00:00:00Z\""
        );

        Broadcast broadcast = new Broadcast(
                "channelUri",
                new DateTime(2016, 1, 5, 22, 0, 0, DateTimeZone.UTC),
                new DateTime(2016, 1, 6, 0, 0, 0, DateTimeZone.UTC)
        );

        ImmutableSet<Broadcast> merge = merger.merge(
                ImmutableSet.of(broadcast),
                ImmutableSet.of(),
                true
        );

        assertThat(merge.contains(broadcast), is(true));
    }

    @Test
    public void mergerKeepsZeroDurationUpdateBroadcastAtStartOfInterval() throws Exception {
        BroadcastMerger merger = BroadcastMerger.parse(
                "\"channelUri\"|\"2016-01-05T00:00:00Z\"|\"2016-01-06T00:00:00Z\""
        );

        Broadcast broadcast = new Broadcast(
                "channelUri",
                new DateTime(2016, 1, 5, 0, 0, 0, DateTimeZone.UTC),
                new DateTime(2016, 1, 5, 0, 0, 0, DateTimeZone.UTC)
        );

        ImmutableSet<Broadcast> merge = merger.merge(
                ImmutableSet.of(broadcast),
                ImmutableSet.of(),
                true
        );

        assertThat(merge.contains(broadcast), is(true));
    }

    @Test
    public void mergerKeepsZeroDurationUpdateBroadcastAtEndOfInterval() throws Exception {
        BroadcastMerger merger = BroadcastMerger.parse(
                "\"channelUri\"|\"2016-01-05T00:00:00Z\"|\"2016-01-06T00:00:00Z\""
        );

        Broadcast broadcast = new Broadcast(
                "channelUri",
                new DateTime(2016, 1, 6, 0, 0, 0, DateTimeZone.UTC),
                new DateTime(2016, 1, 6, 0, 0, 0, DateTimeZone.UTC)
        );

        ImmutableSet<Broadcast> merge = merger.merge(
                ImmutableSet.of(broadcast),
                ImmutableSet.of(),
                true
        );

        assertThat(merge.contains(broadcast), is(true));
    }

    @Test
    public void mergerConsidersNonOverlappingUpdateBroadcastInvalid() throws Exception {
        BroadcastMerger merger = BroadcastMerger.parse(
                "\"channelUri\"|\"2016-01-05T00:00:00Z\"|\"2016-01-06T00:00:00Z\""
        );

        Broadcast broadcast = new Broadcast(
                "channelUri",
                new DateTime(2016, 1, 6, 22, 0, 0, DateTimeZone.UTC),
                new DateTime(2016, 1, 7, 2, 0, 0, DateTimeZone.UTC)
        );

        exception.expect(IllegalArgumentException.class);
        merger.merge(
                ImmutableSet.of(broadcast),
                ImmutableSet.of(),
                true
        );
    }

    @Test
    public void mergerConsidersPartiallyOverlappingUpdateBroadcastInvalid() throws Exception {
        BroadcastMerger merger = BroadcastMerger.parse(
                "\"channelUri\"|\"2016-01-05T00:00:00Z\"|\"2016-01-06T00:00:00Z\""
        );

        Broadcast broadcast = new Broadcast(
                "channelUri",
                new DateTime(2016, 1, 5, 22, 0, 0, DateTimeZone.UTC),
                new DateTime(2016, 1, 6, 2, 0, 0, DateTimeZone.UTC)
        );

        exception.expect(IllegalArgumentException.class);
        merger.merge(
                ImmutableSet.of(broadcast),
                ImmutableSet.of(),
                true
        );
    }

    @Test
    public void mergerConsidersUpdateBroadcastOnDifferentChannelInvalid() throws Exception {
        BroadcastMerger merger = BroadcastMerger.parse(
                "\"channelUri\"|\"2016-01-05T00:00:00Z\"|\"2016-01-06T00:00:00Z\""
        );

        Broadcast broadcast = new Broadcast(
                "otherChannelUri",
                new DateTime(2016, 1, 5, 22, 0, 0, DateTimeZone.UTC),
                new DateTime(2016, 1, 5, 23, 0, 0, DateTimeZone.UTC)
        );

        exception.expect(IllegalArgumentException.class);
        merger.merge(
                ImmutableSet.of(broadcast),
                ImmutableSet.of(),
                true
        );
    }

    @Test
    public void ifMergeIsFalseKeepsAllUpdateBroadcastsAndNoExistingOnes() throws Exception {
        BroadcastMerger merger = BroadcastMerger.parse(
                "\"channelUri\"|\"2016-01-05T00:00:00Z\"|\"2016-01-06T00:00:00Z\""
        );

        Broadcast existingBroadcast = new Broadcast(
                "channelUri",
                new DateTime(2016, 1, 5, 22, 0, 0, DateTimeZone.UTC),
                new DateTime(2016, 1, 5, 23, 0, 0, DateTimeZone.UTC)
        );
        Broadcast updateBroadcast = new Broadcast(
                "channelUri",
                new DateTime(2016, 1, 6, 20, 0, 0, DateTimeZone.UTC),
                new DateTime(2016, 1, 6, 22, 0, 0, DateTimeZone.UTC)
        );

        ImmutableSet<Broadcast> merge = merger.merge(
                ImmutableSet.of(updateBroadcast),
                ImmutableSet.of(existingBroadcast),
                false
        );

        assertThat(merge.contains(updateBroadcast), is(true));
        assertThat(merge.contains(existingBroadcast), is(false));
    }
}

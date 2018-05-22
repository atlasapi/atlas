package org.atlasapi.query.content.merge;

import com.google.api.client.repackaged.com.google.common.base.Strings;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.stream.MoreCollectors;
import org.atlasapi.media.entity.Broadcast;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class BroadcastMerger {

    private static final Splitter ASSERTION_LIST_SPLITTER = Splitter.on("\",\"");
    private static final Splitter ASSERTION_SPLITTER = Splitter.on("\"|\"");

    private final Optional<ImmutableMultimap<String, BroadcastAssertion>> channelAssertions;

    private BroadcastMerger(
            Optional<ImmutableMultimap<String, BroadcastAssertion>> channelAssertions) {
        this.channelAssertions = checkNotNull(channelAssertions);

        if (this.channelAssertions.isPresent()) {
            checkArgument(!this.channelAssertions.get().isEmpty());
        }
    }

    public static BroadcastMerger defaultMerger() {
        return new BroadcastMerger(Optional.empty());
    }

    public static BroadcastMerger parse(@Nullable String broadcastAssertionsParameter) {
        if (Strings.isNullOrEmpty(broadcastAssertionsParameter)) {
            return new BroadcastMerger(Optional.empty());
        }

        String trimmedParameter = broadcastAssertionsParameter.trim();

        checkArgument(trimmedParameter.startsWith("\""));
        checkArgument(trimmedParameter.endsWith("\""));

        String parameter = trimmedParameter.substring(
                1, trimmedParameter.length() - 1
        );

        ImmutableListMultimap<String, BroadcastAssertion> assertions = ASSERTION_LIST_SPLITTER
                .splitToList(parameter)
                .stream()
                .map(BroadcastMerger::parseAssertion)
                .collect(MoreCollectors.toImmutableListMultiMap(
                        BroadcastAssertion::getChannelUri,
                        Function.identity()
                ));

        return new BroadcastMerger(Optional.of(assertions));
    }

    private static BroadcastAssertion parseAssertion(String assertion) {
        List<String> parsedAssertion = ASSERTION_SPLITTER.splitToList(assertion);

        checkArgument(parsedAssertion.size() == 3);

        return BroadcastAssertion.create(
                parsedAssertion.get(0),
                DateTime.parse(parsedAssertion.get(1)),
                DateTime.parse(parsedAssertion.get(2))
        );
    }

    public ImmutableSet<Broadcast> merge(
            Set<Broadcast> update,
            Set<Broadcast> existing,
            boolean merge
    ) {
        if (!merge) {
            return ImmutableSet.copyOf(update);
        }

        boolean updateBroadcastsValid = update
                .stream()
                .allMatch(this::isValidUpdateBroadcast);

        if (!updateBroadcastsValid) {
            throw new IllegalArgumentException("Found broadcasts in the request body that are"
                    + " outside the broadcast assertion interval");
        }

        ImmutableSet<Broadcast> broadcastsToPreserve = existing
                .stream()
                .filter(this::shouldPreserveExistingBroadcast)
                .collect(MoreCollectors.toImmutableSet());

        return ImmutableSet.<Broadcast>builder()
                .addAll(update)
                .addAll(broadcastsToPreserve)
                .build();
    }

    private Boolean shouldPreserveExistingBroadcast(Broadcast broadcast) {
        if (!channelAssertions.isPresent()) {
            // If we have no assertions all existing broadcasts should be kept
            return true;
        }

        ImmutableCollection<BroadcastAssertion> assertions =
                channelAssertions.get().get(broadcast.getBroadcastOn());

        if (assertions.isEmpty()) {
            return true;
        }

        Interval broadcastInterval = new Interval(
                broadcast.getTransmissionTime(),
                broadcast.getTransmissionEndTime()
        );

        return assertions.stream()
                .noneMatch(assertion -> assertion.overlaps(broadcastInterval));
    }

    private Boolean isValidUpdateBroadcast(Broadcast broadcast) {
        if (!channelAssertions.isPresent()) {
            // If we have no assertions all update broadcasts are invalid
            return false;
        }

        ImmutableCollection<BroadcastAssertion> assertions =
                channelAssertions.get().get(broadcast.getBroadcastOn());

        if (assertions.isEmpty()) {
            return false;
        }

        Interval broadcastInterval = new Interval(
                broadcast.getTransmissionTime(),
                broadcast.getTransmissionEndTime()
        );

        return assertions.stream()
                .anyMatch(assertion -> assertion.contains(broadcastInterval));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BroadcastMerger that = (BroadcastMerger) o;
        return Objects.equals(channelAssertions, that.channelAssertions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(channelAssertions);
    }

    private static class BroadcastAssertion {

        private final String channelUri;
        private final Interval interval;

        private BroadcastAssertion(String channelUri, DateTime from, DateTime to) {
            this.channelUri = checkNotNull(channelUri);

            checkNotNull(from);
            checkNotNull(to);
            checkArgument(from.isBefore(to));

            this.interval = new Interval(from, to);
        }

        public static BroadcastAssertion create(String channelId, DateTime from, DateTime to) {
            return new BroadcastAssertion(channelId, from, to);
        }

        public String getChannelUri() {
            return channelUri;
        }

        public boolean overlaps(Interval interval) {
            return this.interval.overlaps(interval);
        }

        public boolean contains(Interval interval) {
            return this.interval.contains(interval)
                    || isZeroDurationAtEndOfThisInterval(interval);
        }

        private boolean isZeroDurationAtEndOfThisInterval(Interval interval) {
            return interval.toDurationMillis() == 0L
                    && interval.getStart().equals(this.interval.getEnd());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            BroadcastAssertion that = (BroadcastAssertion) o;
            return Objects.equals(channelUri, that.channelUri) &&
                    Objects.equals(interval, that.interval);
        }

        @Override
        public int hashCode() {
            return Objects.hash(channelUri, interval);
        }
    }
}

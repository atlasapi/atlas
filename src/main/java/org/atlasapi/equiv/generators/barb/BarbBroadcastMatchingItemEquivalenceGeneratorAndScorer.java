package org.atlasapi.equiv.generators.barb;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.stream.MoreCollectors;
import org.atlasapi.equiv.generators.BroadcastMatchingItemEquivalenceGeneratorAndScorer;
import org.atlasapi.equiv.generators.EquivalenceGenerator;
import org.atlasapi.equiv.generators.metadata.EquivalenceGeneratorMetadata;
import org.atlasapi.equiv.generators.metadata.SourceLimitedEquivalenceGeneratorMetadata;
import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates.Builder;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeComponent;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResult;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Schedule;
import org.atlasapi.media.entity.Schedule.ScheduleChannel;
import org.atlasapi.media.entity.Version;
import org.atlasapi.persistence.content.ScheduleResolver;
import org.joda.time.DateTime;
import org.joda.time.Duration;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.atlasapi.equiv.generators.barb.utils.BarbGeneratorUtils.expandChannelUris;
import static org.atlasapi.equiv.generators.barb.utils.BarbGeneratorUtils.hasQualifyingBroadcast;

/**
 * This is similar to {@link BroadcastMatchingItemEquivalenceGeneratorAndScorer}
 * For certain channels it also generates candidates from additional channels.
 * A matching broadcasts threshold value is calculated for each candidate which is a ratio of the number of
 * broadcasts which matched between subject and candidate, divided by the minimum of the number of total valid
 * broadcasts (actively published and subject to the broadcast filter) of either. This value is then passed
 * to the provided matchingBroadcastsThresholdFunction and will lead to a scoreOnMatch score if the function returns
 * true, scoreOnPartialMatch if it returns false but at least one broadcast matched, or will otherwise lead to the
 * candidate being discarded.
 */
public class BarbBroadcastMatchingItemEquivalenceGeneratorAndScorer implements EquivalenceGenerator<Item> {

    private final ScheduleResolver resolver;
    private final Set<Publisher> supportedPublishers;
    private final Duration flexibility;
    private final ChannelResolver channelResolver;
    private final Predicate<? super Broadcast> broadcastFilter;
    private final Duration SHORT_CONTENT_REDUCED_TIME_FLEXIBILITY = Duration.standardMinutes(10);
    private final Score scoreOnMatch;
    private final Score scoreOnPartialMatch;
    private final Function<Double, Boolean> matchingBroadcastsThresholdFunction;

    public BarbBroadcastMatchingItemEquivalenceGeneratorAndScorer(
            ScheduleResolver resolver,
            ChannelResolver channelResolver,
            Set<Publisher> supportedPublishers,
            Duration flexibility,
            @Nullable Predicate<? super Broadcast> broadcastFilter,
            Score scoreOnMatch,
            Score scoreOnPartialMatch,
            @Nullable Function<Double, Boolean> matchingBroadcastsThresholdFunction
    ) {
        this.resolver = checkNotNull(resolver);
        this.channelResolver = checkNotNull(channelResolver);
        this.supportedPublishers = checkNotNull(supportedPublishers);
        this.flexibility = checkNotNull(flexibility);
        this.broadcastFilter = broadcastFilter == null ? broadcast -> true : broadcastFilter;
        this.scoreOnMatch = checkNotNull(scoreOnMatch);
        this.scoreOnPartialMatch = checkNotNull(scoreOnPartialMatch);
        this.matchingBroadcastsThresholdFunction = matchingBroadcastsThresholdFunction == null
                ? threshold -> true
                : matchingBroadcastsThresholdFunction;
    }

    @Override
    public ScoredCandidates<Item> generate(
            Item subject,
            ResultDescription desc,
            EquivToTelescopeResult equivToTelescopeResult
    ) {

        Builder<Item> scores = DefaultScoredCandidates.fromSource("BARB Broadcast");
        Map<Item, BroadcastMatchingInfo> broadcastMatchingInfoMap = new HashMap<>();

        EquivToTelescopeComponent generatorComponent = EquivToTelescopeComponent.create();
        generatorComponent.setComponentName("BARB Broadcast Matching Item Equivalence Generator");

        Set<Publisher> validPublishers = Sets.difference(
                supportedPublishers,
                ImmutableSet.of(subject.getPublisher())
        );

        int processedBroadcasts = 0;
        int totalBroadcasts = 0;

        int totalNumberOfValidBroadcasts = getTotalNumberOfValidBroadcasts(subject);

        for (Version version : subject.getVersions()) {
            for (Broadcast broadcast : version.getBroadcasts()) {
                totalBroadcasts++;
                if (isValidBroadcast(broadcast)) {
                    processedBroadcasts++;
                    findMatchesForBroadcast(
                            broadcastMatchingInfoMap,
                            totalNumberOfValidBroadcasts,
                            broadcast,
                            validPublishers
                    );
                }
            }
        }

        desc.appendText("Processed %s of %s broadcasts", processedBroadcasts, totalBroadcasts);

        desc.startStage("Checking matching broadcast ratios");
        for (Map.Entry<Item, BroadcastMatchingInfo> candidateEntry : broadcastMatchingInfoMap.entrySet()) {
            Item candidate = candidateEntry.getKey();
            BroadcastMatchingInfo info = candidateEntry.getValue();
            Optional<Score> score = Optional.empty();
            desc.appendText(
                    String.format("%s : %.2f", candidate.getCanonicalUri(), info.getRatioOfMatchingBroadcasts())
            );
            if (matchingBroadcastsThresholdFunction.apply(info.getRatioOfMatchingBroadcasts())) {
                score = Optional.of(scoreOnMatch);
            } else if (info.getNumberOfMatchingBroadcasts() > 0) {
                score = Optional.of(scoreOnPartialMatch);
            }
            if (score.isPresent()) {
                scores.addEquivalent(candidate, score.get());
                if (candidate.getId() != null) {
                    generatorComponent.addComponentResult(
                            candidate.getId(),
                            scoreOnMatch.toString()
                    );
                }
            }
        }
        desc.finishStage();

        equivToTelescopeResult.addGeneratorResult(generatorComponent);

        return scores.build();
    }

    private int getTotalNumberOfValidBroadcasts(Item item) {
        int count = 0;
        for (Version version : item.getVersions()) {
            for (Broadcast broadcast : version.getBroadcasts()) {
                if (isValidBroadcast(broadcast)) {
                    count++;
                }
            }
        }
        return count;
    }

    private boolean isValidBroadcast(Broadcast broadcast) {
        return broadcast.isActivelyPublished() && broadcastFilter.test(broadcast);
    }

    private void findMatchesForBroadcast(
            Map<Item, BroadcastMatchingInfo> broadcastMatchingInfoMap,
            int subjectTotalNumberOfValidBroadcasts,
            Broadcast broadcast,
            Set<Publisher> validPublishers
    ) {
        Set<String> channelUris = expandChannelUris(broadcast.getBroadcastOn());
        Schedule schedule = scheduleAround(broadcast, channelUris, validPublishers);
        for (ScheduleChannel channel : schedule.scheduleChannels()) {
            for (Item scheduleItem : channel.items()) {
                boolean matchingBroadcast = scheduleItem instanceof Item
                        && scheduleItem.isActivelyPublished()
                        && hasQualifyingBroadcast(scheduleItem, broadcast, flexibility);
                updateBroadcastMatchingInfo(
                        broadcastMatchingInfoMap,
                        subjectTotalNumberOfValidBroadcasts,
                        scheduleItem,
                        matchingBroadcast
                );
            }
        }
    }

    private Schedule scheduleAround(Broadcast broadcast, Set<String> channelUris, Set<Publisher> publishers) {
        Duration shortBroadcastFlexibility = Duration.standardMinutes(2);
        Duration broadcastPeriod = new Duration(
                broadcast.getTransmissionTime(),
                broadcast.getTransmissionEndTime()
        );

        DateTime start = broadcast.getTransmissionTime().minus(flexibility);
        DateTime end = broadcast.getTransmissionEndTime().plus(flexibility);

        // if the broadcast is less than 10 minutes long, reduce the flexibility
        if (broadcastPeriod.compareTo(SHORT_CONTENT_REDUCED_TIME_FLEXIBILITY) < 0) {
            start = broadcast.getTransmissionTime().minus(shortBroadcastFlexibility);
            end = broadcast.getTransmissionEndTime().plus(shortBroadcastFlexibility);
        }
        Set<Channel> channels = channelUris.parallelStream()
                .map(channelResolver::fromUri)
                .filter(Maybe::hasValue)
                .map(Maybe::requireValue)
                .collect(MoreCollectors.toImmutableSet());
        return resolver.unmergedSchedule(
                start,
                end,
                channels,
                publishers
        );
    }

    private void updateBroadcastMatchingInfo(
            Map<Item, BroadcastMatchingInfo> broadcastMatchingInfoMap,
            int subjectTotalNumberOfValidBroadcasts,
            Item scheduleItem,
            boolean matchingBroadcast
    ) {
        broadcastMatchingInfoMap.compute(
                scheduleItem,
                (k, v) -> {
                    BroadcastMatchingInfo info;
                    if (v == null) {
                        int numberOfValidBroadcasts =
                                Math.min(
                                        subjectTotalNumberOfValidBroadcasts,
                                        getTotalNumberOfValidBroadcasts(scheduleItem)
                                );
                        info = new BroadcastMatchingInfo(numberOfValidBroadcasts);
                    } else {
                        info = v;
                    }
                    if (matchingBroadcast) {
                        info.incrementNumberOfMatchingBroadcasts();
                    }
                    return info;
                }
        );
    }

    @Override
    public EquivalenceGeneratorMetadata getMetadata() {
        return SourceLimitedEquivalenceGeneratorMetadata.create(
                this.getClass().getCanonicalName(),
                supportedPublishers
        );
    }

    @Override
    public String toString() {
        return "BARB Broadcast-matching generator";
    }

    private class BroadcastMatchingInfo {
        private int numberOfMatchingBroadcasts;
        private final int totalNumberOfBroadcasts;

        public BroadcastMatchingInfo(Integer totalNumberOfBroadcasts) {
            this.totalNumberOfBroadcasts = checkNotNull(totalNumberOfBroadcasts);
        }

        public int incrementNumberOfMatchingBroadcasts() {
            return ++numberOfMatchingBroadcasts;
        }

        public int getNumberOfMatchingBroadcasts() {
            return numberOfMatchingBroadcasts;
        }

        public int getTotalNumberOfBroadcasts() {
            return totalNumberOfBroadcasts;
        }

        public double getRatioOfMatchingBroadcasts() {
            return ((double) numberOfMatchingBroadcasts) / totalNumberOfBroadcasts;
        }
    }
}

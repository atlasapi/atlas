package org.atlasapi.equiv.generators.aenetworks;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Sets;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.stream.MoreCollectors;
import org.atlasapi.equiv.generators.EquivalenceGenerator;
import org.atlasapi.equiv.generators.metadata.EquivalenceGeneratorMetadata;
import org.atlasapi.equiv.generators.metadata.SourceLimitedEquivalenceGeneratorMetadata;
import org.atlasapi.equiv.results.description.NopDescription;
import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.equiv.scorers.barb.BarbTitleMatchingItemScorer;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeComponent;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResult;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.entity.*;
import org.atlasapi.media.entity.Schedule.ScheduleChannel;
import org.atlasapi.persistence.content.ScheduleResolver;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.atlasapi.equiv.generators.barb.utils.BarbGeneratorUtils.*;

public class AeBroadcastMatchingItemEquivalenceGeneratorAndScorer implements EquivalenceGenerator<Item> {
    private static final Logger log = LoggerFactory.getLogger(BarbTitleMatchingItemScorer.class);
    private static final String TXLOG_EPISODE_TITLE_CUSTOM_FIELD_NAME = "txlog:episode_title";

    private final ScheduleResolver scheduleResolver;
    private final Set<Publisher> supportedPublishers;
    private final ChannelResolver channelResolver;
    private final Predicate<? super Broadcast> broadcastFilter;
    private final Score scoreOnMatch;
    private final Duration scheduleWindow; //for offset calculation
    private final Duration broadcastFlexibility;
    private final Duration shortBroadcastFlexibility;
    private final Duration shortBroadcastMaxDuration;
    private final NopDescription nopDesc;

    private AeBroadcastMatchingItemEquivalenceGeneratorAndScorer(Builder builder) {
        scheduleResolver = checkNotNull(builder.scheduleResolver);
        supportedPublishers = checkNotNull(builder.supportedPublishers);
        channelResolver = checkNotNull(builder.channelResolver);
        broadcastFilter = checkNotNull(builder.broadcastFilter);
        scoreOnMatch = checkNotNull(builder.scoreOnMatch);
        scheduleWindow = checkNotNull(builder.scheduleWindow);
        broadcastFlexibility = checkNotNull(builder.broadcastFlexibility);
        shortBroadcastFlexibility = checkNotNull(builder.shortBroadcastFlexibility);
        checkArgument(!shortBroadcastFlexibility.isLongerThan(broadcastFlexibility));
        shortBroadcastMaxDuration = checkNotNull(builder.shortBroadcastMaxDuration);
        nopDesc = new NopDescription();
    }

    public static Builder builder() {
        return new Builder();
    }


    @Override
    public ScoredCandidates<Item> generate(
            Item subject,
            ResultDescription desc,
            EquivToTelescopeResult equivToTelescopeResult
    ) {

        DefaultScoredCandidates.Builder<Item> scores = DefaultScoredCandidates.fromSource("A+E Broadcast");

        EquivToTelescopeComponent generatorComponent = EquivToTelescopeComponent.create();
        generatorComponent.setComponentName("A+E Broadcast Matching Item Equivalence Generator");

        Set<Publisher> validPublishers = Sets.difference(
                supportedPublishers,
                ImmutableSet.of(subject.getPublisher())
        );

        int processedBroadcasts = 0;
        int totalBroadcasts = 0;

        for (Version version : subject.getVersions()) {
            for (Broadcast broadcast : version.getBroadcasts()) {
                totalBroadcasts++;
                if (broadcast.isActivelyPublished() && broadcastFilter.test(broadcast)) {
                    processedBroadcasts++;
                    findMatchesForBroadcast(
                            scores,
                            broadcast,
                            subject.getPublisher(),
                            validPublishers,
                            desc,
                            generatorComponent
                    );
                }
            }
        }

        equivToTelescopeResult.addGeneratorResult(generatorComponent);

        desc.appendText("Processed %s of %s broadcasts", processedBroadcasts, totalBroadcasts);

        return scores.build();
    }

    @Override
    public EquivalenceGeneratorMetadata getMetadata() {
        return SourceLimitedEquivalenceGeneratorMetadata.create(
                this.getClass().getCanonicalName(),
                supportedPublishers
        );
    }

    /**
     * Resolves the subject's schedule and the candidate schedules for the given subject's broadcast.
     */
    private void findMatchesForBroadcast(
            DefaultScoredCandidates.Builder<Item> scores,
            Broadcast subjectBroadcast,
            Publisher subjectPublisher,
            Set<Publisher> validPublishers,
            ResultDescription desc,
            EquivToTelescopeComponent generatorComponent
    ) {
        desc.startStage(
                "Finding matches  A+E broadcast: " + String.format(
                        "%s [%s - %s]",
                        subjectBroadcast.getBroadcastOn(),
                        subjectBroadcast.getTransmissionTime(),
                        subjectBroadcast.getTransmissionEndTime())
        );
        Set<String> channelUris = expandChannelUris(subjectBroadcast.getBroadcastOn());
        Schedule subjectSchedule = scheduleAround(
                subjectBroadcast,
                ImmutableSet.of(subjectBroadcast.getBroadcastOn())
                , ImmutableSet.of(subjectPublisher)
        );
        ScheduleChannel subjectScheduleChannel = subjectSchedule.scheduleChannels().iterator().next();

        Item[] subjectItemArray = subjectScheduleChannel.items().toArray(new Item[0]);
        int subjectItemIndex = findSubjectInSchedule(subjectItemArray, subjectBroadcast);
        if (subjectItemIndex < 0) {
            desc.appendText("Could not find subject item in the schedule");
            desc.finishStage();
            return;
        }

        Schedule candidateSchedule = scheduleAround(subjectBroadcast, channelUris, validPublishers);
        for (ScheduleChannel candidateScheduleChannel : candidateSchedule.scheduleChannels()) {
            if (candidateScheduleChannel.items().isEmpty()) {
                continue;
            }
            ListMultimap<Publisher, Item> publisherItems = filterScheduleByPublisher(candidateScheduleChannel);
            for (Publisher publisher : publisherItems.keySet()) {
                desc.startStage(
                        "Candidate schedule found for" + publisher.key()
                                + " for " + candidateScheduleChannel.channel().getUri()
                );

                Item[] candidateItemArray = publisherItems.get(publisher).toArray(new Item[0]);
                int candidateItemIndex = findClosestCandidateInSchedule(
                        candidateItemArray,
                        subjectBroadcast,
                        nopDesc
                );
                if (candidateItemIndex < 0) {
                    StringBuilder sb = new StringBuilder();
                    sb.append(String.format("Could not find any suitable candidate in the schedule - time mismatch. \nA+E transmission was between [%s - %s]",
                            subjectBroadcast.getTransmissionTime(),
                            subjectBroadcast.getTransmissionEndTime()
                    ));
                    for (Item candidate : candidateItemArray) {
                        Broadcast candidateBroadcast = getBroadcastFromScheduleItem(candidate, nopDesc);
                        sb.append(String.format("\nCandidate %s had a broadcast between [%s - %s], series title %s and episode title %s",
                                candidate.getCanonicalUri(),
                                candidateBroadcast.getTransmissionTime(),
                                candidateBroadcast.getTransmissionEndTime(),
                                candidate.getTitle(),
                                candidate.getCustomField(TXLOG_EPISODE_TITLE_CUSTOM_FIELD_NAME)
                        ));
                    }
                    desc.appendText(sb.toString());
                    desc.finishStage();
                    continue;
                }

                Item candidate = candidateItemArray[candidateItemIndex];
                if (candidate.isActivelyPublished()) {
                    Broadcast candidateBroadcast = getBroadcastFromScheduleItem(candidate, nopDesc);
                    desc.appendText(
                            "Found candidate %s with broadcast [%s - %s], series title %s and episode title %s",
                            candidate.getCanonicalUri(),
                            candidateBroadcast.getTransmissionTime(),
                            candidateBroadcast.getTransmissionEndTime(),
                            candidate.getTitle(),
                            candidate.getCustomField(TXLOG_EPISODE_TITLE_CUSTOM_FIELD_NAME)
                    );
                    if (candidateBroadcast.isActivelyPublished() && broadcastFilter.test(candidateBroadcast)) {
                        //we want the maximum score for this scorer to be scoreOnMatch, so we update the
                        //score of a candidate instead of adding it up via the usual .addEquivalent()
                        scores.updateEquivalent(candidate, scoreOnMatch);

                        if (candidate.getId() != null) {
                            generatorComponent.addComponentResult(
                                    candidate.getId(),
                                    scoreOnMatch.toString()
                            );
                        }
                    } else {
                        desc.appendText("Candidate broadcast did not pass the broadcast filter");
                    }
                }
                desc.finishStage();
            }
        }
        desc.finishStage();
    }

    private ListMultimap<Publisher, Item> filterScheduleByPublisher(ScheduleChannel candidateScheduleChannel) {
        return candidateScheduleChannel.items().stream() // N.B. we need to retain the order
                .collect(MoreCollectors.toImmutableListMultiMap(Item::getPublisher, item -> item));
    }

    private Schedule scheduleAround(Broadcast broadcast, Set<String> channelUris, Set<Publisher> publishers) {
        DateTime start = broadcast.getTransmissionTime().minus(scheduleWindow);
        DateTime end = broadcast.getTransmissionEndTime().plus(scheduleWindow);

        Set<Channel> channels = channelUris.parallelStream()
                .map(channelResolver::fromUri)
                .filter(Maybe::hasValue)
                .map(Maybe::requireValue)
                .collect(MoreCollectors.toImmutableSet());
        return scheduleResolver.unmergedSchedule(
                start,
                end,
                channels,
                publishers
        );
    }

    /**
     * Find the Item whose schedule slot has the same start and end time as the subject broadcast.
     */
    private int findSubjectInSchedule(Item[] scheduleItems, Broadcast subjectBroadcast) {
        if (scheduleItems.length <= 0) {
            return -1;
        }
        int start = scheduleItems.length / 2; //it's probably around the middle
        if (itemMatchesBroadcast(scheduleItems[start], subjectBroadcast)) {
            return start;
        }
        int d = 1;
        int l = start - d;
        int r = start + d;
        while (l >= 0 || r < scheduleItems.length) {
            if (l >= 0 && itemMatchesBroadcast(scheduleItems[l], subjectBroadcast)) {
                return l;
            }
            if (r < scheduleItems.length && itemMatchesBroadcast(scheduleItems[r], subjectBroadcast)) {
                return r;
            }
            d++;
            l = start - d;
            r = start + d;
        }
        return -1;
    }

    private boolean itemMatchesBroadcast(Item item, Broadcast broadcast) {
        if (item.isActivelyPublished()
                && hasQualifyingBroadcast(item, broadcast, Duration.ZERO, broadcastFilter)) {
            return true;
        }
        return false;
    }

    /**
     * Find the Item whose schedule slot start time is closest to the subject broadcast's start time which satisfies
     * the specified broadcast flexibility.
     */
    private int findClosestCandidateInSchedule(
            Item[] scheduleItems,
            Broadcast subjectBroadcast,
            ResultDescription desc
    ) {
        if (scheduleItems.length <= 0) {
            return -1;
        }
        int bestCandidateFound = -1;
        Duration shortestDurationOffset = null;
        for (int i = 0; i < scheduleItems.length; i++) {
            Item candidate = scheduleItems[i];
            Broadcast candidateBroadcast = getBroadcastFromScheduleItem(candidate, desc);
            Duration offset;
            if (subjectBroadcast.getTransmissionTime().isAfter(candidateBroadcast.getTransmissionTime())) {
                offset = new Duration(
                        candidateBroadcast.getTransmissionTime(),
                        subjectBroadcast.getTransmissionTime()
                );
            } else {
                offset = new Duration(
                        subjectBroadcast.getTransmissionTime(),
                        candidateBroadcast.getTransmissionTime()
                );
            }
            boolean shorterOffset = shortestDurationOffset == null || offset.isShorterThan(shortestDurationOffset);
            boolean withinFlexibility = around(
                    subjectBroadcast,
                    candidateBroadcast,
                    getFlexibility(subjectBroadcast, candidateBroadcast)
            );
            if (shorterOffset && withinFlexibility) {
                shortestDurationOffset = offset;
                bestCandidateFound = i;
            }
        }
        return bestCandidateFound;
    }

    private Broadcast getBroadcastFromScheduleItem(Item item, ResultDescription desc) {
        List<Broadcast> broadcasts = item.getVersions().stream()
                .map(Version::getBroadcasts)
                .flatMap(Collection::stream)
                .filter(Broadcast::isActivelyPublished)
                .collect(MoreCollectors.toImmutableList());

        //The schedule scheduleResolver should return items each with a single broadcast corresponding to their schedule slot
        if (broadcasts.size() != 1) {
            desc.appendText(
                    "Expected one broadcast but found multiple for schedule item %s", item.getCanonicalUri());
        }
        return broadcasts.get(0);
    }

    private Duration getFlexibility(Broadcast... broadcasts) {
        for (Broadcast broadcast : broadcasts) {
            Duration broadcastPeriod = new Duration(
                    broadcast.getTransmissionTime(),
                    broadcast.getTransmissionEndTime()
            );
            // if the broadcast is less than 10 minutes long, reduce the flexibility
            if (!broadcastPeriod.isLongerThan(shortBroadcastMaxDuration)) {
                return shortBroadcastFlexibility;
            }
        }
        return broadcastFlexibility;
    }

    @Override
    public String toString() {
        return "A+E Broadcast-matching generator";
    }

    public static final class Builder {
        private ScheduleResolver scheduleResolver;
        private Set<Publisher> supportedPublishers;
        private ChannelResolver channelResolver;
        private Predicate<? super Broadcast> broadcastFilter = broadcast -> true;
        private Score scoreOnMatch;
        private Duration scheduleWindow;
        private Duration broadcastFlexibility;
        private Duration shortBroadcastFlexibility;
        private Duration shortBroadcastMaxDuration;

        private Builder() {
        }

        public Builder withScheduleResolver(ScheduleResolver resolver) {
            this.scheduleResolver = resolver;
            return this;
        }

        public Builder withSupportedPublishers(Set<Publisher> supportedPublishers) {
            this.supportedPublishers = supportedPublishers;
            return this;
        }

        public Builder withChannelResolver(ChannelResolver channelResolver) {
            this.channelResolver = channelResolver;
            return this;
        }

        /**
         * Optional
         */
        public Builder withBroadcastFilter(Predicate<? super Broadcast> broadcastFilter) {
            this.broadcastFilter = broadcastFilter;
            return this;
        }

        public Builder withScoreOnMatch(Score scoreOnMatch) {
            this.scoreOnMatch = scoreOnMatch;
            return this;
        }

        public Builder withScheduleWindow(Duration scheduleWindow) {
            this.scheduleWindow = scheduleWindow;
            return this;
        }

        public Builder withBroadcastFlexibility(Duration broadcastFlexibility) {
            this.broadcastFlexibility = broadcastFlexibility;
            return this;
        }

        public Builder withShortBroadcastFlexibility(Duration shortBroadcastFlexibility) {
            this.shortBroadcastFlexibility = shortBroadcastFlexibility;
            return this;
        }

        public Builder withShortBroadcastMaxDuration(Duration shortBroadcastMaxDuration) {
            this.shortBroadcastMaxDuration = shortBroadcastMaxDuration;
            return this;
        }

        public AeBroadcastMatchingItemEquivalenceGeneratorAndScorer build() {
            return new AeBroadcastMatchingItemEquivalenceGeneratorAndScorer(this);
        }
    }
}

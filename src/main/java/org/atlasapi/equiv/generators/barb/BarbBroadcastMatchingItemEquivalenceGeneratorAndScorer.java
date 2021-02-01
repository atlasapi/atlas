package org.atlasapi.equiv.generators.barb;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Sets;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.stream.MoreCollectors;
import org.atlasapi.equiv.generators.BroadcastMatchingItemEquivalenceGeneratorAndScorer;
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
import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Schedule;
import org.atlasapi.media.entity.Schedule.ScheduleChannel;
import org.atlasapi.media.entity.Version;
import org.atlasapi.persistence.content.ScheduleResolver;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.atlasapi.equiv.generators.barb.utils.BarbGeneratorUtils.around;
import static org.atlasapi.equiv.generators.barb.utils.BarbGeneratorUtils.expandChannelUris;
import static org.atlasapi.equiv.generators.barb.utils.BarbGeneratorUtils.hasQualifyingBroadcast;

/**
 * This is a heavily adapted take on {@link BroadcastMatchingItemEquivalenceGeneratorAndScorer}.
 * For certain channels it also generates candidates from additional channels e.g. BBC Two England
 * is a single channel in Nitro but multiple regional channels in TxLogs.
 *
 * This generator aims to deal with edge cases where multiple episodes of the same show are broadcast multiple times,
 * and the schedules of the subject and candidate are offset a slight amount with the end result that the closest
 * matching broadcast is actually for a different episode of the same show. Since txlog title scoring often uses
 * the brand titles, this could easily lead to the wrong episodes being equived together. To solve this we try to
 * identify if the subject and candidate exist as part of such a block of multiple episodes, and if so take the candidate
 * whose position in its block is the same as the position of the subject within its block.
 */
public class BarbBroadcastMatchingItemEquivalenceGeneratorAndScorer implements EquivalenceGenerator<Item> {
    private static final Logger log = LoggerFactory.getLogger(BarbTitleMatchingItemScorer.class);

    private final ScheduleResolver scheduleResolver;
    private final Set<Publisher> supportedPublishers;
    private final ChannelResolver channelResolver;
    private final Predicate<? super Broadcast> broadcastFilter;
    private final Score scoreOnMatch;
    private final Duration scheduleWindow; //for offset calculation
    private final Duration broadcastFlexibility;
    private final Duration shortBroadcastFlexibility;
    private final Duration shortBroadcastMaxDuration;
    private final BarbTitleMatchingItemScorer titleMatchingScorer;
    private final NopDescription nopDesc;

    private BarbBroadcastMatchingItemEquivalenceGeneratorAndScorer(Builder builder) {
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
        titleMatchingScorer = checkNotNull(builder.titleMatchingScorer);
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

        DefaultScoredCandidates.Builder<Item> scores = DefaultScoredCandidates.fromSource("BARB Broadcast");

        EquivToTelescopeComponent generatorComponent = EquivToTelescopeComponent.create();
        generatorComponent.setComponentName("BARB Broadcast Matching Item Equivalence Generator");

        if (!TieredBroadcaster.isTierOneOrAAndE(subject)) {
            desc.appendText("Item not from an allowed broadcaster, ignoring all broadcasts.");
            return scores.build();
        }

        Set<Publisher> validPublishers = Sets.difference(
                supportedPublishers,
                ImmutableSet.of(subject.getPublisher())
        );

        int processedBroadcasts = 0;
        int totalBroadcasts = 0;

        for (Version version : subject.getVersions()) {
            int broadcastCount = version.getBroadcasts().size();
            for (Broadcast broadcast : version.getBroadcasts()) {
                totalBroadcasts++;
                if (broadcast.isActivelyPublished() && broadcastFilter.test(broadcast)) {
                    processedBroadcasts++;
                    findMatchesForBroadcast(
                            scores,
                            subject,
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
     * Resolves the subject's schedule and the candidate schedules for the given subject's broadcast; for each candidate
     * schedule finds the block of similar episodes which contains a broadcast within the specified flexibility to the
     * subject broadcast. Compares this block with the block containing the subject episode and if the same size will
     * generate and score the candidate whose position in the block is the same as the subject within its block.
     */
    private void findMatchesForBroadcast(
            DefaultScoredCandidates.Builder<Item> scores,
            Item subject,
            Broadcast subjectBroadcast,
            Publisher subjectPublisher,
            Set<Publisher> validPublishers,
            ResultDescription desc,
            EquivToTelescopeComponent generatorComponent
    ) {
        desc.startStage(
                "Finding matches for broadcast: " + String.format(
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
                        "Candidate schedule found for " + publisher.key()
                                + " for " + candidateScheduleChannel.channel().getUri()
                );

                Item[] candidateItemArray = publisherItems.get(publisher).toArray(new Item[0]);
                int candidateItemIndex = findClosestCandidateInSchedule(
                        candidateItemArray,
                        subject,
                        subjectBroadcast,
                        nopDesc
                );
                if (candidateItemIndex < 0) {
                    desc.appendText("Could not find any suitable candidate in the schedule");
                    desc.finishStage();
                    continue;
                }

                Optional<Item> foundCandidate = checkBlocksAndDetermineCandidate(
                        subjectItemArray,
                        subjectItemIndex,
                        candidateItemArray,
                        candidateItemIndex,
                        nopDesc
                );
                if (!foundCandidate.isPresent()) {
                    desc.appendText("Could not determine candidate from schedule");
                    desc.finishStage();
                    continue;
                }

                Item candidate = foundCandidate.get();
                if (candidate.isActivelyPublished()) {
                    Broadcast candidateBroadcast = getBroadcastFromScheduleItem(candidate, nopDesc);
                    desc.appendText(
                            "Found candidate %s with broadcast [%s - %s]",
                            candidate.getCanonicalUri(),
                            candidateBroadcast.getTransmissionTime(),
                            candidateBroadcast.getTransmissionEndTime()
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
     * the specified broadcast flexibility and has the same title as the subject as determined by the title matching
     * scorer.
     */
    private int findClosestCandidateInSchedule(
            Item[] scheduleItems,
            Item subject,
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
                if (isRealPositiveScore(titleMatchingScorer.score(subject, candidate, desc))) {
                    shortestDurationOffset = offset;
                    bestCandidateFound = i;
                }
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

    /**
     * Determines the start and end of the block of similar episodes containing the subject and candidate;
     * If the block sizes are the same and do not extend past the corresponding list of schedule items the candidate
     * item is returned that is in the same position within its block as the subject is in its. Otherwise no item is
     * returned.
     */
    private Optional<Item> checkBlocksAndDetermineCandidate(
            Item[] subjectItemArray,
            int subjectItemPosition,
            Item[] candidateItemArray,
            int candidateItemPosition,
            ResultDescription desc
    ) {
        Item subject = subjectItemArray[subjectItemPosition];
        Item possibleCandidate = candidateItemArray[candidateItemPosition];

        int subjectPreviousBlockEnd = subjectItemPosition - 1;
        while (subjectPreviousBlockEnd >= 0
                && similarEpisodes(
                subject,
                subjectItemArray[subjectPreviousBlockEnd],
                desc
        )
        ) {
            subjectPreviousBlockEnd--;
        }

        int candidatePreviousBlockEnd = candidateItemPosition - 1;
        while (candidatePreviousBlockEnd >= 0
                && similarEpisodes(
                possibleCandidate,
                candidateItemArray[candidatePreviousBlockEnd],
                desc
        )
        ) {
            candidatePreviousBlockEnd--;
        }

        if ((subjectPreviousBlockEnd < 0 || candidatePreviousBlockEnd < 0)) {
            return Optional.empty();
        }

        int subjectNextBlockStart = subjectItemPosition + 1;
        while (subjectNextBlockStart < subjectItemArray.length
                && similarEpisodes(
                subject,
                subjectItemArray[subjectNextBlockStart],
                desc
        )
        ) {
            subjectNextBlockStart++;
        }

        int candidateNextBlockStart = candidateItemPosition + 1;
        while (candidateNextBlockStart < candidateItemArray.length
                && similarEpisodes(
                possibleCandidate,
                candidateItemArray[candidateNextBlockStart],
                desc
        )
        ) {
            candidateNextBlockStart++;
        }

        if (subjectNextBlockStart >= subjectItemArray.length
                || candidateNextBlockStart >= candidateItemArray.length) {
            return Optional.empty();
        }

        int subjectBlockDifference = subjectNextBlockStart - subjectPreviousBlockEnd;
        int candidateBlockDifference = candidateNextBlockStart - candidatePreviousBlockEnd;

        if (subjectBlockDifference != candidateBlockDifference) {
            return Optional.empty();
        }

        int subjectPositionInBlock = subjectItemPosition - subjectPreviousBlockEnd; //1-indexed
        int foundCandidatePosition = candidatePreviousBlockEnd + subjectPositionInBlock;

        return Optional.of(candidateItemArray[foundCandidatePosition]);
    }

    private boolean isRealPositiveScore(Score score) {
        return score.isRealScore() && score.asDouble() > 0D;
    }

    private boolean similarEpisodes(Item subject, Item candidate, ResultDescription desc) {
        if (subject.getContainer() != null || candidate.getContainer() != null) {
            return Objects.equals(subject.getContainer(), candidate.getContainer());
        }
        return isRealPositiveScore(
                titleMatchingScorer.score(
                        subject,
                        candidate,
                        desc
                )
        );
    }

    @Override
    public String toString() {
        return "BARB Broadcast-matching generator";
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
        private BarbTitleMatchingItemScorer titleMatchingScorer;

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

        public Builder withTitleMatchingScorer(BarbTitleMatchingItemScorer titleMatchingScorer) {
            this.titleMatchingScorer = titleMatchingScorer;
            return this;
        }

        public BarbBroadcastMatchingItemEquivalenceGeneratorAndScorer build() {
            return new BarbBroadcastMatchingItemEquivalenceGeneratorAndScorer(this);
        }
    }
}

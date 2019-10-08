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

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.atlasapi.equiv.generators.barb.utils.BarbGeneratorUtils.expandChannelUris;
import static org.atlasapi.equiv.generators.barb.utils.BarbGeneratorUtils.hasQualifyingBroadcast;

/**
 * This is mostly a copy of {@link BroadcastMatchingItemEquivalenceGeneratorAndScorer} class but with some logic
 * stripped out. For certain channels it also generates candidates from additional channels. Highest
 * score is taken for each candidate (as opposed to summing them as per regular broadcast generator)
 */
public class BarbBroadcastMatchingItemEquivalenceGeneratorAndScorer implements EquivalenceGenerator<Item> {

    private final ScheduleResolver resolver;
    private final Set<Publisher> supportedPublishers;
    private final ChannelResolver channelResolver;
    private final Predicate<? super Broadcast> broadcastFilter;
    private final Score scoreOnMatch;
    private final Duration scheduleWindow; //for offset calculation
    private final BarbTitleMatchingItemScorer titleMatchingScorer;

    public BarbBroadcastMatchingItemEquivalenceGeneratorAndScorer(
            ScheduleResolver resolver,
            ChannelResolver channelResolver,
            Set<Publisher> supportedPublishers,
            Duration scheduleWindow,
            Predicate<? super Broadcast> broadcastFilter,
            Score scoreOnMatch,
            BarbTitleMatchingItemScorer titleMatchingScorer
    ) {
        this.resolver = checkNotNull(resolver);
        this.channelResolver = checkNotNull(channelResolver);
        this.supportedPublishers = checkNotNull(supportedPublishers);
        this.broadcastFilter = broadcastFilter == null ? broadcast -> true : broadcastFilter;
        this.scoreOnMatch = checkNotNull(scoreOnMatch);
        this.titleMatchingScorer = checkNotNull(titleMatchingScorer);
        this.scheduleWindow = checkNotNull(scheduleWindow);
    }

    @Override
    public ScoredCandidates<Item> generate(
            Item subject,
            ResultDescription desc,
            EquivToTelescopeResult equivToTelescopeResult
    ) {

        Builder<Item> scores = DefaultScoredCandidates.fromSource("BARB Broadcast");

        EquivToTelescopeComponent generatorComponent = EquivToTelescopeComponent.create();
        generatorComponent.setComponentName("BARB Broadcast Matching Item Equivalence Generator");

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

    private void findMatchesForBroadcast(
            Builder<Item> scores,
            Item subject,
            Broadcast subjectBroadcast,
            Publisher subjectPublisher,
            Set<Publisher> validPublishers,
            ResultDescription desc,
            EquivToTelescopeComponent generatorComponent
    ) {
        desc.startStage(
                "Schedule for " + String.format(
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
        Schedule candidateSchedule = scheduleAround(subjectBroadcast, channelUris, validPublishers);

        List<Item> subjectItemList = subjectScheduleChannel.items();
        Item[] subjectItemArray = subjectItemList.toArray(new Item[0]);
        int subjectItemIndex = findItemInArray(subjectItemArray, subjectBroadcast);
        if (subjectItemIndex < 0) {
            desc.appendText("Could not find subject item in the schedule");
            desc.finishStage();
            return;
        }
        for (ScheduleChannel candidateScheduleChannel : candidateSchedule.scheduleChannels()) {
            List<Item> candidateItemList = candidateScheduleChannel.items();
            if (candidateItemList.isEmpty()) {
                continue;
            }
            desc.startStage("Resolving candidate schedule for " + candidateScheduleChannel.channel().getUri());
            Item[] candidateItemArray = candidateItemList.toArray(new Item[0]);
            int candidateItemIndex = findSuitableCandidateInArray(candidateItemArray, subject, subjectBroadcast, desc);
            if (candidateItemIndex < 0) {
                desc.appendText("Could not find a suitable item in the candidate schedule");
                desc.finishStage();
                continue;
            }
            Optional<Item> foundCandidate = findCandidate(
                    subjectItemArray,
                    subjectItemIndex,
                    candidateItemArray,
                    candidateItemIndex,
                    desc
            );
            if (!foundCandidate.isPresent()) {
                desc.appendText("Could not determine candidate from schedule");
                desc.finishStage();
                continue;
            }

            Item candidate = foundCandidate.get();
            if(candidate.isActivelyPublished()) {
                //we want the maximum score for this scorer to be scoreOnMatch, so we update the
                //score of a candidate instead of adding it up via the usual .addEquivalent()
                scores.updateEquivalent(candidate, scoreOnMatch);

                if (candidate.getId() != null) {
                    generatorComponent.addComponentResult(
                            candidate.getId(),
                            scoreOnMatch.toString()
                    );
                }
            }
            desc.finishStage();
        }
        desc.finishStage();
    }

    private Schedule scheduleAround(Broadcast broadcast, Set<String> channelUris, Set<Publisher> publishers) {
        DateTime start = broadcast.getTransmissionTime().minus(scheduleWindow);
        DateTime end = broadcast.getTransmissionEndTime().plus(scheduleWindow);

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

    private int findItemInArray(Item[] items, Broadcast broadcast) {
        if (items.length <= 0) {
            return -1;
        }
        int start = items.length / 2; //it's probably around the middle
        if (itemMatchesBroadcast(items[start], broadcast)) {
            return start;
        }
        int d = 1;
        int l = start - d;
        int r = start + d;
        while (l >= 0 || r < items.length) {
            if (l >= 0 && itemMatchesBroadcast(items[l], broadcast)) {
                return l;
            }
            if (r < items.length && itemMatchesBroadcast(items[r], broadcast)) {
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

    private int findSuitableCandidateInArray(
            Item[] items,
            Item subject,
            Broadcast subjectBroadcast,
            ResultDescription desc
    ) {
        if (items.length <= 0) {
            return -1;
        }
        int bestCandidateFound = -1;
        Duration shortestDurationOffset = null;
        for (int i = 0; i < items.length; i++) {
            Item candidate = items[i];
            //The schedule resolver should return items each with a single broadcast corresponding to their schedule slot
            Broadcast candidateBroadcast = candidate.getVersions().iterator().next().getBroadcasts().iterator().next();
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
            if (shortestDurationOffset == null || offset.isShorterThan(shortestDurationOffset)) {
                if (isRealPositiveScore(titleMatchingScorer.score(subject, candidate, desc))) {
                    shortestDurationOffset = offset;
                    bestCandidateFound = i;
                }
            }
        }
        return bestCandidateFound;
    }

    private Optional<Item> findCandidate(
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
                && isRealPositiveScore(
                titleMatchingScorer.score(
                        subject,
                        subjectItemArray[subjectPreviousBlockEnd],
                        desc
                )
        )) {
            subjectPreviousBlockEnd--;
        }

        int candidatePreviousBlockEnd = candidateItemPosition - 1;
        while (candidatePreviousBlockEnd >= 0
                && isRealPositiveScore(
                titleMatchingScorer.score(
                        possibleCandidate,
                        candidateItemArray[candidatePreviousBlockEnd],
                        desc
                )
        )) {
            candidatePreviousBlockEnd--;
        }

        if ((subjectPreviousBlockEnd < 0 && candidatePreviousBlockEnd >= 0)
                || (candidatePreviousBlockEnd < 0 && subjectPreviousBlockEnd >= 0)
        ) {
            return Optional.empty();
        }

        int subjectNextBlockStart = subjectItemPosition + 1;
        while (subjectNextBlockStart < subjectItemArray.length
                && isRealPositiveScore(
                titleMatchingScorer.score(
                        subject,
                        subjectItemArray[subjectNextBlockStart],
                        desc
                )
        )) {
            subjectNextBlockStart++;
        }

        int candidateNextBlockStart = candidateItemPosition + 1;
        while (candidateNextBlockStart < candidateItemArray.length
                && isRealPositiveScore(
                titleMatchingScorer.score(
                        possibleCandidate,
                        candidateItemArray[candidateNextBlockStart],
                        desc
                )
        )) {
            candidateNextBlockStart++;
        }

        if ((subjectNextBlockStart >= subjectItemArray.length && candidateNextBlockStart < candidateItemArray.length)
                || (candidateNextBlockStart >= candidateItemArray.length && subjectNextBlockStart < subjectItemArray.length)
        ) {
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


    @Override
    public String toString() {
        return "BARB Broadcast-matching generator";
    }
}

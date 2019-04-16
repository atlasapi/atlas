package org.atlasapi.equiv.results.extractors;

import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.stream.MoreCollectors;
import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.ScoredCandidate;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeComponent;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResult;
import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Series;
import org.joda.time.DateTime;
import org.joda.time.Duration;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * This extractor attempts to get multiple candidates for the same publisher. It will only do this
 * if
 * <ul>
 *     <li>the target is not a brand or a series</li>
 *     <li>the top scoring candidate is not a brand or a series</li>
 *     <li>it can find a group of candidates whose score is close (within
 *     {@link MultipleCandidateExtractor#BROADCAST_TIME_FLEXIBILITY}) to the top scoring candidate
 *     and that have at least one broadcast that either contains or are contained by any
 *     target broadcast (+- {@link MultipleCandidateExtractor#BROADCAST_TIME_FLEXIBILITY})</li>
 * </ul>
 */
public class MultipleCandidateExtractor<T extends Content> implements EquivalenceExtractor<T>{

    private static final Duration BROADCAST_TIME_FLEXIBILITY = Duration.standardMinutes(5);
    private static final double PUBLISHER_MATCHING_EQUIV_THRESHOLD = 0.3;

    private MultipleCandidateExtractor() {
    }

    public static <T extends Content> MultipleCandidateExtractor<T> create() {
        return new MultipleCandidateExtractor<>();
    }

    @Override
    public Set<ScoredCandidate<T>> extract(
            List<ScoredCandidate<T>> candidates,
            T target,
            ResultDescription desc,
            EquivToTelescopeResult equivToTelescopeResult
    ) {
        EquivToTelescopeComponent extractorComponent = EquivToTelescopeComponent.create();
        extractorComponent.setComponentName("Multiple Candidate Extractor");

        if (isSeriesOrBrand(target) || candidates.isEmpty()) {
            return ImmutableSet.of();
        }

        ScoredCandidate<T> highestScoringCandidate = candidates.get(0);

        if (isSeriesOrBrand(highestScoringCandidate.candidate())) {
            return ImmutableSet.of();
        }

        Set<ScoredCandidate<T>> allowedCandidates = new HashSet<ScoredCandidate<T>>();

        allowedCandidates.add(highestScoringCandidate);

        ImmutableSet<ScoredCandidate<T>> filteredCandidates = candidates.stream()
                .filter(candidate -> !isSeriesOrBrand(candidate.candidate()))
                .collect(MoreCollectors.toImmutableSet());

        ImmutableSet<ScoredCandidate<T>> matchedCandidates = filteredCandidates
                .stream()
                .filter(candidate -> Math.abs(
                        candidate.score().asDouble() - highestScoringCandidate.score().asDouble()
                ) < PUBLISHER_MATCHING_EQUIV_THRESHOLD)
                .filter(candidate -> broadcastsMatchCheck(target, candidate))
                .collect(MoreCollectors.toImmutableSet());

        allowedCandidates.addAll(matchedCandidates);

        allowedCandidates.forEach(candidate -> {
                    if (candidate.candidate().getId() != null) {
                        extractorComponent.addComponentResult(
                                candidate.candidate().getId(),
                                String.valueOf(candidate.score().asDouble())
                        );
                    }
                }
        );

        equivToTelescopeResult.addExtractorResult(extractorComponent);

        if (allowedCandidates.size() > 1) {
            return allowedCandidates;
        } else {
            return ImmutableSet.of();
        }
    }

    private boolean isSeriesOrBrand(T target) {
        return (target instanceof Brand || target instanceof Series);
    }

    private boolean broadcastsMatchCheck(
            T target,
            ScoredCandidate<T> candidate
    ) {
        return target.getVersions().stream()
                .flatMap(version -> version.getBroadcasts().stream())
                .anyMatch(broadcast -> broadcastsMatchCheck(candidate.candidate(), broadcast))
                ||
                candidate.candidate().getVersions().stream()
                        .flatMap(version -> version.getBroadcasts().stream())
                        .anyMatch(broadcast -> broadcastsMatchCheck(target, broadcast));
    }

    private boolean broadcastsMatchCheck(T candidate, Broadcast broadcast) {
        return candidate.getVersions().stream()
                .flatMap(version -> version.getBroadcasts().stream())
                .anyMatch(broadcast1 -> broadcastsMatchCheckInclusive(broadcast1, broadcast));
    }

    private boolean broadcastsMatchCheckInclusive(Broadcast broadcastOne, Broadcast broadcastTwo) {
        DateTime broadcastTransmissionStart = broadcastOne.getTransmissionTime();
        DateTime broadcastTransmissionEnd = broadcastOne.getTransmissionEndTime();
        DateTime broadcastTwoTransmissionStart = broadcastTwo.getTransmissionTime();
        DateTime broadcastTwoTransmissionEnd = broadcastTwo.getTransmissionEndTime();

        // Check if first broadcast is contained within the second with some flexibility
        return broadcastTransmissionStart.isAfter(
                broadcastTwoTransmissionStart.minus(BROADCAST_TIME_FLEXIBILITY))
                &&
                broadcastTransmissionEnd.isBefore(
                        broadcastTwoTransmissionEnd.plus(BROADCAST_TIME_FLEXIBILITY));
    }
}

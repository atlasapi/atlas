package org.atlasapi.equiv.results.extractors;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidate;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeComponent;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResults;
import org.atlasapi.media.entity.Content;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;

/**
 * This extractor will select all candidates which score over the highest threshold that would extract at least one candidate
 */
public class AllOverOrEqHighestNonEmptyThresholdExtractor<T extends Content> implements EquivalenceExtractor<T> {

    private final SortedSet<Double> thresholds;

    public AllOverOrEqHighestNonEmptyThresholdExtractor(Collection<Double> thresholds) {
        this.thresholds = ImmutableSortedSet.copyOf(Comparator.reverseOrder(), thresholds); //highest first
    }

    @Override
    public Set<ScoredCandidate<T>> extract(
            List<ScoredCandidate<T>> candidates,
            T target,
            ResultDescription desc,
            EquivToTelescopeResults equivToTelescopeResults
    ) {
        EquivToTelescopeComponent extractorComponent = EquivToTelescopeComponent.create();
        extractorComponent.setComponentName("All over >= " + thresholds.toString());

        if (candidates.isEmpty()) {
            return ImmutableSet.of();
        }

        ImmutableSet.Builder<ScoredCandidate<T>> allowedCandidatesBuilder = ImmutableSet.builder();

        Score highestScore = getHighestScore(candidates);
        if (highestScore.isRealScore()) {
            Optional<Double> highestMatchingThreshold = getHighestMatchingThreshold(highestScore.asDouble());
            if (highestMatchingThreshold.isPresent()) {
                double threshold = highestMatchingThreshold.get();
                for (ScoredCandidate<T> candidate : candidates) {
                    if (candidate.score().asDouble() >= threshold) {
                        allowedCandidatesBuilder.add(candidate);
                        if (candidate.candidate().getId() != null) {
                            extractorComponent.addComponentResult(
                                    candidate.candidate().getId(),
                                    String.valueOf(candidate.score().asDouble())
                            );
                        }
                    }
                }
            }
        }

        equivToTelescopeResults.addExtractorResult(extractorComponent);
        return allowedCandidatesBuilder.build();
    }

    private Score getHighestScore(List<ScoredCandidate<T>> candidates) {
        Score highestScore = Score.nullScore();
        for (ScoredCandidate<T> candidate : candidates) {
            if(candidate.score().isRealScore()
                    && (!highestScore.isRealScore() || candidate.score().asDouble() > highestScore.asDouble())) {
                highestScore = candidate.score();
            }
        }
        return highestScore;
    }

    private Optional<Double> getHighestMatchingThreshold(double highestScore) {
        Optional<Double> highestMatchingThreshold = Optional.empty();
        for (double threshold : thresholds) {
            if (highestScore >= threshold) {
                highestMatchingThreshold = Optional.of(threshold);
                break;
            }
        }
        return highestMatchingThreshold;
    }
}

package org.atlasapi.equiv.results.extractors;

import java.util.List;
import java.util.Set;

import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.ScoredCandidate;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeComponent;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResults;
import org.atlasapi.media.entity.Content;

import com.google.common.collect.ImmutableSet;

/**
 * This extractor will select all candidates at the best score from the publisher of the target
 * content, which are at least over or at the given threshold.
 */
public class AllWithTheSameHighscoreAndPublisherExtractor<T extends Content> implements EquivalenceExtractor<T> {

    private final double threshold;

    private AllWithTheSameHighscoreAndPublisherExtractor(double threshold) {
        this.threshold = threshold;
    }

    public static <T extends Content> AllWithTheSameHighscoreAndPublisherExtractor<T> create(double threshold) {
        return new AllWithTheSameHighscoreAndPublisherExtractor<>(threshold);
    }

    @Override
    public Set<ScoredCandidate<T>> extract(
            List<ScoredCandidate<T>> candidates,
            T target,
            ResultDescription desc,
            EquivToTelescopeResults equivToTelescopeResults
    ) {
        EquivToTelescopeComponent extractorComponent = EquivToTelescopeComponent.create();
        extractorComponent.setComponentName("All at the top from the content's publisher and >= "
                                            + threshold);

        if (candidates.isEmpty()) {
            return ImmutableSet.of();
        }

        //find the highscore
        Double highScore = null;
        for (ScoredCandidate<T> candidate : candidates) {
            if (candidate.candidate().getPublisher().equals(target.getPublisher())) {
                highScore = candidate.score().asDouble();
                break;
            }
        }
        if (highScore == null || highScore < threshold) {
            return ImmutableSet.of();
        }

        //find all of the same publisher at the same highscore
        ImmutableSet.Builder<ScoredCandidate<T>> allowedCandidatesBuilder = ImmutableSet.builder();
        for (ScoredCandidate<T> candidate : candidates) {
            if (candidate.score().asDouble() == highScore
                && candidate.candidate().getPublisher() == target.getPublisher()) {
                allowedCandidatesBuilder.add(candidate);

                //presentation notes
                if (candidate.candidate().getId() != null) {
                    extractorComponent.addComponentResult(
                            candidate.candidate().getId(),
                            String.valueOf(candidate.score().asDouble())
                    );
                }
            }
        }

        equivToTelescopeResults.addExtractorResult(extractorComponent);
        return allowedCandidatesBuilder.build();
    }
}
package org.atlasapi.equiv.results.extractors;

import com.google.common.collect.ImmutableSet;
import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.ScoredCandidate;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeComponent;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResult;
import org.atlasapi.media.entity.Content;

import java.util.List;
import java.util.Set;

/**
 * This extractor will select all candidates that tie at the top score, over or equal to the
 * given threshold. If nothing ties at the top, this returns nothing.
 */
public class AllThatTieAtTopExtractor<T extends Content> implements EquivalenceExtractor<T> {

    private final double threshold;

    private AllThatTieAtTopExtractor(double threshold) {
        this.threshold = threshold;
    }

    public static <T extends Content> AllThatTieAtTopExtractor<T> create(double threshold) {
        return new AllThatTieAtTopExtractor<>(threshold);
    }

    @Override
    public Set<ScoredCandidate<T>> extract(
            List<ScoredCandidate<T>> candidates,
            T target,
            ResultDescription desc,
            EquivToTelescopeResult equivToTelescopeResult
    ) {
        EquivToTelescopeComponent extractorComponent = EquivToTelescopeComponent.create();
        extractorComponent.setComponentName("All that tie at the top and >= " + threshold);

        if (candidates.isEmpty()) {
            return ImmutableSet.of();
        }

        ScoredCandidate<T> highestScoringCandidate = candidates.get(0);

        if (highestScoringCandidate.score().asDouble() < threshold) {
            return ImmutableSet.of();
        }

        ImmutableSet.Builder<ScoredCandidate<T>> allowedCandidatesBuilder = ImmutableSet.builder();
        for (ScoredCandidate<T> candidate : candidates) {
            if (candidate.score().asDouble() == highestScoringCandidate.score().asDouble()) {
                allowedCandidatesBuilder.add(candidate);
            }
        }

        ImmutableSet<ScoredCandidate<T>> allowedCandidates = allowedCandidatesBuilder.build();
        //if its only 1, then nothing ties at the top of the list, and this fails.
        if (allowedCandidates.size() > 1) {
            //keep notes for result presentation.
            for (ScoredCandidate<T> candidate : allowedCandidates) {
                if (candidate.candidate().getId() != null) {
                    extractorComponent.addComponentResult(
                            candidate.candidate().getId(),
                            String.valueOf(candidate.score().asDouble())
                    );
                }
            }
            equivToTelescopeResult.addExtractorResult(extractorComponent);
            return allowedCandidates;
        } else {
            return ImmutableSet.of();
        }
    }
}

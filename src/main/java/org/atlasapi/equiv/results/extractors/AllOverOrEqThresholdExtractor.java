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
 * This extractor will select all candidates over or at the given threshold.
 */
public class AllOverOrEqThresholdExtractor<T extends Content> implements EquivalenceExtractor<T> {

    private final double threshold;

    private AllOverOrEqThresholdExtractor(double threshold) {
        this.threshold = threshold;
    }

    public static <T extends Content> AllOverOrEqThresholdExtractor<T> create(double threshold) {
        return new AllOverOrEqThresholdExtractor<>(threshold);
    }

    @Override
    public Set<ScoredCandidate<T>> extract(
            List<ScoredCandidate<T>> candidates,
            T target,
            ResultDescription desc,
            EquivToTelescopeResult equivToTelescopeResult
    ) {
        EquivToTelescopeComponent extractorComponent = EquivToTelescopeComponent.create();
        extractorComponent.setComponentName("All over >= " + threshold);

        if (candidates.isEmpty()) {
            return ImmutableSet.of();
        }

        ImmutableSet.Builder<ScoredCandidate<T>> allowedCandidatesBuilder = ImmutableSet.builder();
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

        equivToTelescopeResult.addExtractorResult(extractorComponent);
        return allowedCandidatesBuilder.build();
    }
}

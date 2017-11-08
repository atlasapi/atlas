package org.atlasapi.equiv.results.extractors;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.ScoredCandidate;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeComponent;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResults;
import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Series;

import com.google.common.collect.ImmutableSet;
import org.joda.time.DateTime;

/**
 * This extractor will get all select all candidates that tie at the top score, over or equal to the
 * given threshold. If nothing ties at the top, this returns nothing.
 */
public class AllWithTheSameHighScore<T extends Content> implements EquivalenceExtractor<T> {

    private final double threshold;

    private AllWithTheSameHighScore(double threshold) {
        this.threshold = threshold;
    }

    public static <T extends Content> AllWithTheSameHighScore<T> create(double threshold) {
        return new AllWithTheSameHighScore<>(threshold);
    }

    @Override
    public Set<ScoredCandidate<T>> extract(
            List<ScoredCandidate<T>> candidates,
            T target,
            ResultDescription desc,
            EquivToTelescopeResults equivToTelescopeResults
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

        Set<ScoredCandidate<T>> allowedCandidates = new HashSet<>();
        for (ScoredCandidate<T> candidate : candidates) {
            if (candidate.score().asDouble() == highestScoringCandidate.score().asDouble()) {
                allowedCandidates.add(highestScoringCandidate);
                //keep notes for result presentation.
                if (candidate.candidate().getId() != null) {
                    extractorComponent.addComponentResult(
                            candidate.candidate().getId(),
                            String.valueOf(candidate.score().asDouble())
                    );
                }
            }
        }

        equivToTelescopeResults.addExtractorResult(extractorComponent);

        //if its only 1, then nothing ties at the top of the list, and this fails.
        if (allowedCandidates.size() > 1) {
            return allowedCandidates;
        } else {
            return ImmutableSet.of();
        }
    }
}
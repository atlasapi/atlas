package org.atlasapi.equiv.results.extractors;

import java.util.List;

import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.ScoredCandidate;

import com.google.common.base.Optional;

/**
 * Selects the equivalence with the highest score, so long as its score
 * is more than threshold times larger than the next highest-scoring
 * candidate.
 */
public class PercentThresholdAboveNextBestMatchEquivalenceExtractor<T> implements EquivalenceExtractor<T> {

    private static final String NAME = "Percent Above Next Best Match Extractor";
    private final double threshold;

    public static <T> PercentThresholdAboveNextBestMatchEquivalenceExtractor<T> atLeastNTimesGreater(double threshold) {
        return new PercentThresholdAboveNextBestMatchEquivalenceExtractor<T>(threshold);
    }

    private PercentThresholdAboveNextBestMatchEquivalenceExtractor(double threshold) {
        this.threshold = threshold;
    }
    
    @Override
    public Optional<ScoredCandidate<T>> extract(List<ScoredCandidate<T>> candidates, T subject,
            ResultDescription desc) {
        desc.startStage(NAME);
        
        if (candidates.isEmpty()) {
            desc.appendText("no equivalents").finishStage();
            return Optional.absent();
        }
        
        if (candidates.size() == 1) {
            return Optional.of(candidates.get(0));
        }
        
        ScoredCandidate<T> strongest = candidates.get(0);
        ScoredCandidate<T> nextBest = candidates.get(1);
        
        if (!strongest.score().isRealScore()) {
            desc.appendText("%s not extracted. Not a real score.", strongest.candidate());
            return Optional.absent();
        }
        
        if (!nextBest.score().isRealScore()) {
            desc.appendText("%s extracted; next best score is not real", strongest.candidate());
            return Optional.of(strongest);
        }
        
        if ( (strongest.score().asDouble() / nextBest.score().asDouble()) >= threshold) {
            desc.appendText("%s extracted. Strongest score of %s wins over next best from %s of %s.", strongest.candidate(), strongest.score(), 
                    nextBest.candidate(), nextBest.score());
            return Optional.of(strongest);
        }
        
        desc.appendText("%s not extracted. Strongest score of %s doesn't beat next best from %s of %s.", strongest.candidate(), strongest.score(), 
                    nextBest.candidate(), nextBest.score());
        
        return Optional.absent();
    }

}

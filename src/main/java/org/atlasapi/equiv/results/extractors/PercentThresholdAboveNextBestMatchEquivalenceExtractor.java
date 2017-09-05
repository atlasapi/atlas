package org.atlasapi.equiv.results.extractors;

import java.util.List;

import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.ScoredCandidate;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeComponent;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResults;
import org.atlasapi.media.entity.Content;

import com.google.common.base.Optional;

/**
 * Selects the equivalence with the highest score, so long as its score
 * is more than threshold times larger than the next highest-scoring
 * candidate.
 */
public class PercentThresholdAboveNextBestMatchEquivalenceExtractor<T>
        implements EquivalenceExtractor<T> {

    private static final String NAME = "Percent Above Next Best Match Extractor";
    private final double threshold;

    public static <T> PercentThresholdAboveNextBestMatchEquivalenceExtractor<T> atLeastNTimesGreater(
            double threshold
    ) {
        return new PercentThresholdAboveNextBestMatchEquivalenceExtractor<T>(threshold);
    }

    private PercentThresholdAboveNextBestMatchEquivalenceExtractor(double threshold) {
        this.threshold = threshold;
    }
    
    @Override
    public Optional<ScoredCandidate<T>> extract(
            List<ScoredCandidate<T>> candidates,
            T subject,
            ResultDescription desc,
            EquivToTelescopeResults equivToTelescopeResults
    ) {
        desc.startStage(NAME);

        EquivToTelescopeComponent extractorComponent = EquivToTelescopeComponent.create();
        extractorComponent.setComponentName(
                "Percent Threshold Above Next Best Match Equivalence Extractor"
        );
        
        if (candidates.isEmpty()) {
            desc.appendText("no equivalents").finishStage();
            return Optional.absent();
        }
        
        if (candidates.size() == 1) {
            extractorComponent.addComponentResult(
                    ((Content) candidates.get(0).candidate()).getId(),
                    String.valueOf(candidates.get(0).score().asDouble())
            );
            equivToTelescopeResults.addExtractorResult(extractorComponent);
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
            extractorComponent.addComponentResult(
                    ((Content) strongest.candidate()).getId(),
                    String.valueOf(strongest.score().asDouble())
            );
            equivToTelescopeResults.addExtractorResult(extractorComponent);
            return Optional.of(strongest);
        }
        
        if ( (strongest.score().asDouble() / nextBest.score().asDouble()) >= threshold) {
            desc.appendText("%s extracted. Strongest score of %s wins over next best from %s of %s.", strongest.candidate(), strongest.score(), 
                    nextBest.candidate(), nextBest.score());
            extractorComponent.addComponentResult(
                    ((Content) strongest.candidate()).getId(),
                    String.valueOf(strongest.score().asDouble())
            );
            equivToTelescopeResults.addExtractorResult(extractorComponent);
            return Optional.of(strongest);
        }
        
        desc.appendText("%s not extracted. Strongest score of %s doesn't beat next best from %s of %s.", strongest.candidate(), strongest.score(), 
                    nextBest.candidate(), nextBest.score());
        
        return Optional.absent();
    }

}

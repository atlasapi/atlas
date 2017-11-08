package org.atlasapi.equiv.results.extractors;

import java.util.List;
import java.util.Set;

import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidate;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeComponent;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResults;
import org.atlasapi.media.entity.Content;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;

/**
 * Selects the candidate with the highest score given its score is above a given percentage threshold of the total of all equivalents' scores
 */
public class PercentThresholdEquivalenceExtractor<T> implements EquivalenceExtractor<T> {
    
    public static <T> PercentThresholdEquivalenceExtractor<T> moreThanPercent(int percent) {
        return new PercentThresholdEquivalenceExtractor<T>(percent/100.0);
    }

    private final Double threshold;

    public PercentThresholdEquivalenceExtractor(Double threshold) {
        this.threshold = threshold;
    }
    
    private static final String NAME = "Percent Extractor";
    
    @Override
    public Set<ScoredCandidate<T>> extract(
            List<ScoredCandidate<T>> candidates,
            T subject,
            ResultDescription desc,
            EquivToTelescopeResults equivToTelescopeResults
    ) {
        EquivToTelescopeComponent extractorCompoenent = EquivToTelescopeComponent.create();
        extractorCompoenent.setComponentName("Percent Threshold Equivalence Extractor");

        desc.startStage(NAME);
        
        if (candidates.isEmpty()) {
            desc.appendText("no equivalents").finishStage();
            return ImmutableSet.of();
        }
        
        Double total = sum(candidates);

        ScoredCandidate<T> strongest = candidates.get(0);
        if (strongest.score().isRealScore() && strongest.score().asDouble() / total > threshold) {
            desc.appendText("%s extracted. %s / %s > %s", strongest.candidate(), strongest.score(), total, threshold).finishStage();
            if (((Content) strongest.candidate()).getId() != null) {
                extractorCompoenent.addComponentResult(
                        ((Content) strongest.candidate()).getId(),
                        String.valueOf(strongest.score().asDouble())
                );
            }
            equivToTelescopeResults.addExtractorResult(extractorCompoenent);
            return ImmutableSet.of(strongest);
        }
        
        desc.appendText("%s not extracted. %s / %s < %s", strongest.candidate(), strongest.score(), total, threshold).finishStage();
        return ImmutableSet.of();
    }

    private Double sum(List<ScoredCandidate<T>> equivalents) {
        Double total = 0.0;
        
        for (ScoredCandidate<T> scoredEquivalent : equivalents) {
            Score score = scoredEquivalent.score();
            if(score.isRealScore() && score.asDouble() > 0) {
                total += score.asDouble();
            }
        }
        
        return total;
    }
    
}

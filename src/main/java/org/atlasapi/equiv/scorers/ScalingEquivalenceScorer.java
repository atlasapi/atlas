package org.atlasapi.equiv.scorers;

import com.google.common.base.Function;
import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.ScaledScoredEquivalents;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResult;
import org.atlasapi.media.entity.Content;

import java.util.Set;

public class ScalingEquivalenceScorer<T extends Content> implements EquivalenceScorer<T> {

    private final EquivalenceScorer<T> delegate;
    private final Function<Double, Double> scalingFunction;


    public static <T extends Content> ScalingEquivalenceScorer<T> scale(EquivalenceScorer<T> delegate, final double scaler) {
        return scale(delegate, new Function<Double, Double>() {
            @Override
            public Double apply(Double input) {
                return input * scaler;
            }
            @Override
            public String toString() {
                return String.format("%+.2f", scaler);
            }
        });
    }
    
    public static <T extends Content> ScalingEquivalenceScorer<T> scale(EquivalenceScorer<T> delegate, Function<Double, Double> scalingFunction) {
      return new ScalingEquivalenceScorer<T>(delegate, scalingFunction);
    }
    
    public ScalingEquivalenceScorer(EquivalenceScorer<T> delegate, Function<Double, Double> scalingFunction) {
        this.delegate = delegate;
        this.scalingFunction = scalingFunction;
    }
    
    @Override
    public ScoredCandidates<T> score(
            T content,
            Set<? extends T> suggestions,
            ResultDescription desc,
            EquivToTelescopeResult equivToTelescopeResult
    ) {
        return ScaledScoredEquivalents.<T>scale(delegate.score(
                content,
                suggestions,
                desc,
                equivToTelescopeResult
        ), scalingFunction);
    }
 
    @Override
    public String toString() {
        return String.format("%s (scaled by %s)", delegate, scalingFunction);
    }
}
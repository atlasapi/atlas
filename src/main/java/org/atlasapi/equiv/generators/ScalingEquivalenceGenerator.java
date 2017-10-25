package org.atlasapi.equiv.generators;

import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.ScaledScoredEquivalents;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResults;
import org.atlasapi.media.entity.Content;

import com.google.common.base.Function;

public class ScalingEquivalenceGenerator<T extends Content> implements EquivalenceGenerator<T> {

    public static <T extends Content> ScalingEquivalenceGenerator<T> scale(
            EquivalenceGenerator<T> delegate,
            final double scaler
    ) {
        return scale(delegate, new Function<Double, Double>() {
            @Override
            public Double apply(Double input) {
                return input * scaler;
            }
            
            @Override
            public String toString() {
                return String.valueOf(scaler);
            }
        });
    }
    
    public static <T extends Content> ScalingEquivalenceGenerator<T> scale(
            EquivalenceGenerator<T> delegate,
            Function<Double, Double> basicScale
    ) {
      return new ScalingEquivalenceGenerator<T>(delegate, basicScale);
    }
    
    private final EquivalenceGenerator<T> delegate;
    private final Function<Double, Double> scalingFunction;

    public ScalingEquivalenceGenerator(
            EquivalenceGenerator<T> delegate,
            Function<Double, Double> scalingFunction
    ) {
        this.delegate = delegate;
        this.scalingFunction = scalingFunction;
    }
    
    @Override
    public ScoredCandidates<T> generate(
            T content,
            ResultDescription desc,
            EquivToTelescopeResults equivToTelescopeResults
    ) {
        return ScaledScoredEquivalents.<T>scale(delegate.generate(
                content,
                desc,
                equivToTelescopeResults
        ), scalingFunction);
    }

    @Override
    public String toString() {
        return String.format("%s (scaled by %s)", delegate, scalingFunction);
    }
}

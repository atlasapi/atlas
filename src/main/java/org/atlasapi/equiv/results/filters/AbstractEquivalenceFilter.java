package org.atlasapi.equiv.results.filters;

import java.util.List;

import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.ScoredCandidate;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

public abstract class AbstractEquivalenceFilter<T> implements EquivalenceFilter<T> {

    @Override
    public final List<ScoredCandidate<T>> apply(
            Iterable<ScoredCandidate<T>> candidates,
            final T subject,
            final ResultDescription desc
    ) {
        desc.startStage(toString());
        Builder<ScoredCandidate<T>> results = ImmutableList.builder();
        for (ScoredCandidate<T> candidate : candidates) {
            if (doFilter(candidate, subject, desc)) {
                results.add(candidate);
            }
        }
        desc.finishStage();
        return results.build();
    }

    protected abstract boolean doFilter(
            ScoredCandidate<T> input,
            T subject,
            ResultDescription desc
    );

    @Override
    public String toString() {
        return this.getClass().getSimpleName();
    }
    
}

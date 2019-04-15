package org.atlasapi.equiv.results.filters;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.ScoredCandidate;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResult;

import java.util.List;

public abstract class AbstractEquivalenceFilter<T> implements EquivalenceFilter<T> {

    @Override
    public final List<ScoredCandidate<T>> apply(
            Iterable<ScoredCandidate<T>> candidates,
            final T subject,
            final ResultDescription desc,
            EquivToTelescopeResult equivToTelescopeResult
    ) {
        desc.startStage(toString());
        Builder<ScoredCandidate<T>> results = ImmutableList.builder();
        for (ScoredCandidate<T> candidate : candidates) {
            if (doFilter(candidate, subject, desc, equivToTelescopeResult)) {
                results.add(candidate);
            }
        }
        desc.finishStage();
        return results.build();
    }

    protected abstract boolean doFilter(
            ScoredCandidate<T> input,
            T subject,
            ResultDescription desc,
            EquivToTelescopeResult equivToTelescopeResult
    );

    @Override
    public String toString() {
        return this.getClass().getSimpleName();
    }
    
}

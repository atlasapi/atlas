package org.atlasapi.equiv.results.filters;

import java.util.List;

import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.ScoredCandidate;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResults;
import org.atlasapi.media.entity.Equiv;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

public abstract class AbstractEquivalenceFilter<T> implements EquivalenceFilter<T> {

    @Override
    public final List<ScoredCandidate<T>> apply(
            Iterable<ScoredCandidate<T>> candidates,
            final T subject,
            final ResultDescription desc,
            EquivToTelescopeResults equivToTelescopeResults
    ) {
        desc.startStage(toString());
        Builder<ScoredCandidate<T>> results = ImmutableList.builder();
        for (ScoredCandidate<T> candidate : candidates) {
            if (doFilter(candidate, subject, desc, equivToTelescopeResults)) {
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
            EquivToTelescopeResults equivToTelescopeResults
    );

    @Override
    public String toString() {
        return this.getClass().getSimpleName();
    }
    
}

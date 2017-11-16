package org.atlasapi.equiv.results.filters;

import java.util.List;

import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.ScoredCandidate;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeComponent;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResults;

import com.google.common.collect.ImmutableList;

public class ConjunctiveFilter<T> implements EquivalenceFilter<T> {

    public static final <T> EquivalenceFilter<T> valueOf(Iterable<? extends EquivalenceFilter<T>> filters) {
        return new ConjunctiveFilter<T>(filters);
    }
    
    private final List<EquivalenceFilter<T>> filters;

    private ConjunctiveFilter(Iterable<? extends EquivalenceFilter<T>> filters) {
        this.filters = ImmutableList.copyOf(filters);
    }

    @Override
    public List<ScoredCandidate<T>> apply(
            Iterable<ScoredCandidate<T>> candidates,
            T subject,
            ResultDescription desc,
            EquivToTelescopeResults equivToTelescopeResults
    ) {
        desc.startStage(toString());
        Iterable<ScoredCandidate<T>> result = candidates;
        for (EquivalenceFilter<T> filter : filters) {
            result = filter.apply(result, subject, desc, equivToTelescopeResults);
        }
        desc.finishStage();
        return ImmutableList.copyOf(result);
    }

    @Override
    public String toString() {
        return "all of: " + filters;
    }
}

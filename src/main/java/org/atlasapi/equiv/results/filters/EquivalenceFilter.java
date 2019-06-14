package org.atlasapi.equiv.results.filters;

import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.ScoredCandidate;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResult;

import java.util.List;

public interface EquivalenceFilter<T> {
    
    List<ScoredCandidate<T>> apply(
            Iterable<ScoredCandidate<T>> candidates,
            T subject,
            ResultDescription desc,
            EquivToTelescopeResult equivToTelescopeResult
    );
    
}

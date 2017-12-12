package org.atlasapi.equiv.results;

import java.util.List;

import org.atlasapi.equiv.results.description.ReadableDescription;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResults;

public interface EquivalenceResultBuilder<T> {

    EquivalenceResult<T> resultFor(
            T target,
            List<ScoredCandidates<T>> equivalents,
            ReadableDescription desc,
            EquivToTelescopeResults equivToTelescopeResults
    );

}
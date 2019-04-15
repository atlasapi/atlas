package org.atlasapi.equiv.results;

import org.atlasapi.equiv.results.description.ReadableDescription;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResult;

import java.util.List;

public interface EquivalenceResultBuilder<T> {

    EquivalenceResult<T> resultFor(
            T target,
            List<ScoredCandidates<T>> equivalents,
            ReadableDescription desc,
            EquivToTelescopeResult equivToTelescopeResult
    );

}
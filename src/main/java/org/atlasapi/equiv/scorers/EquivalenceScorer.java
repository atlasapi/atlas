package org.atlasapi.equiv.scorers;

import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResult;

import java.util.Set;

public interface EquivalenceScorer<T> {

    /**
     * <p>
     * Calculate {@link org.atlasapi.equiv.results.scores.Score Score}s for a
     * set of candidates for the given a subject.
     * </p>
     * 
     * @param subject
     * @param candidates
     * @param desc
     * @return
     */
    ScoredCandidates<T> score(
            T subject,
            Set<? extends T> candidates,
            ResultDescription desc,
            EquivToTelescopeResult equivToTelescopeResult
    );
    
}

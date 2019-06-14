package org.atlasapi.equiv.results.extractors;

import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.ScoredCandidate;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResult;

import java.util.List;
import java.util.Set;


public interface EquivalenceExtractor<T> {

    /**
     * Extracts the equivalent pieces of content from an ordered list of weighted candidates.
     * @param candidates - equivalence candidates for a single publisher, ordered from highest scoring to lowest.
     * @param subject - the subject content
     * @param desc TODO
     * @return strong equivalent or absent if none of the candidates  
     */
    Set<ScoredCandidate<T>> extract(
            List<ScoredCandidate<T>> candidates,
            T subject,
            ResultDescription desc,
            EquivToTelescopeResult equivToTelescopeResult
    );
    
}

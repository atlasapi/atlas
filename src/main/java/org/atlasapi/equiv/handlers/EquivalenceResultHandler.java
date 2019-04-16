package org.atlasapi.equiv.handlers;

import org.atlasapi.equiv.results.EquivalenceResults;

public interface EquivalenceResultHandler<T> {

    /**
     * Handle the equivalence results
     * @param results equivalence results
     * @return false if the handler decided that the result has result in no change to the state
     * of the system and should therefore be ignored, true otherwise
     */
    boolean handle(EquivalenceResults<T> results);
    
}

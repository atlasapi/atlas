package org.atlasapi.equiv.handlers;

import org.atlasapi.equiv.results.EquivalenceResult;

public interface EquivalenceResultHandler<T> {

    /**
     * Handle the equivalence result
     * @param result equivalence result
     * @return false if the handler decided that the result has result in no change to the state
     * of the system and should therefore be ignored, true otherwise
     */
    boolean handle(EquivalenceResult<T> result);
    
}

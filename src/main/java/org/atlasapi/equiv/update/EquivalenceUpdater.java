package org.atlasapi.equiv.update;

public interface EquivalenceUpdater<T> {

    boolean updateEquivalences(T subject);
    
}

package org.atlasapi.equiv.results.persistence;

import org.atlasapi.equiv.results.EquivalenceResults;
import org.atlasapi.media.entity.Content;

import java.util.List;

public interface EquivalenceResultStore {

    <T extends Content> StoredEquivalenceResults store(EquivalenceResults<T> results);
    
    StoredEquivalenceResults forId(String canonicalUri);
    
    List<StoredEquivalenceResults> forIds(Iterable<String> canonicalUris);
}

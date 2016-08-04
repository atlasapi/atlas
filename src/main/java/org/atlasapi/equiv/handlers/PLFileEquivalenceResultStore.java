package org.atlasapi.equiv.handlers;

import java.util.List;

import org.atlasapi.equiv.analytics.EquivResultsReader;
import org.atlasapi.equiv.results.EquivalenceResult;
import org.atlasapi.equiv.results.persistence.EquivalenceResultStore;
import org.atlasapi.equiv.results.persistence.StoredEquivalenceResult;
import org.atlasapi.equiv.results.persistence.StoredEquivalenceResultTranslator;
import org.atlasapi.media.entity.Content;

/**
 * Filesystem-based store for equivalence results. New files will be stored in a
 * hashed directory structure to avoid a single directory with too many files in it, but
 * an attempt will also be made to read directly from the root directory for old results.
 */
public class PLFileEquivalenceResultStore implements EquivalenceResultStore {

    private StoredEquivalenceResultTranslator translator = new StoredEquivalenceResultTranslator();
    private String titleAddOn;

    public PLFileEquivalenceResultStore(String titleAddOn) {
        this.titleAddOn = titleAddOn;
    }

    @Override
    public <T extends Content> StoredEquivalenceResult store(
            EquivalenceResult<T> result) {
        StoredEquivalenceResult storedEquivalenceResult = translator.toStoredEquivalenceResult(result);
        EquivResultsReader reader = new EquivResultsReader(storedEquivalenceResult, titleAddOn);
        try {
            reader.visualiseData();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return storedEquivalenceResult;
    }

    @Override
    public StoredEquivalenceResult forId(String canonicalUri) {
        return null;
    }

    @Override
    public List<StoredEquivalenceResult> forIds(Iterable<String> canonicalUris) {
        return null;
    }

}


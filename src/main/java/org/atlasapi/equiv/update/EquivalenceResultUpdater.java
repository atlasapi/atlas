package org.atlasapi.equiv.update;

import org.atlasapi.equiv.results.EquivalenceResult;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResults;
import org.atlasapi.equiv.update.metadata.EquivalenceUpdaterMetadata;


public interface EquivalenceResultUpdater<T> {

    EquivalenceResult<T> provideEquivalenceResult(T subject, EquivToTelescopeResults resultsForTelescope);

    EquivalenceUpdaterMetadata getMetadata();
}

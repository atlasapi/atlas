package org.atlasapi.equiv.update;

import org.atlasapi.equiv.results.EquivalenceResult;
import org.atlasapi.equiv.results.description.ReadableDescription;
import org.atlasapi.equiv.update.metadata.EquivalenceUpdaterMetadata;
import org.atlasapi.reporting.telescope.OwlTelescopeReporter;


public interface EquivalenceResultUpdater<T> {

    EquivalenceResult<T> provideEquivalenceResult(T subject, OwlTelescopeReporter telescope, ReadableDescription desc);

    EquivalenceUpdaterMetadata getMetadata();
}

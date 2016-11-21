package org.atlasapi.equiv.generators;

import org.atlasapi.equiv.generators.metadata.EquivalenceGeneratorMetadata;
import org.atlasapi.equiv.generators.metadata.GenericEquivalenceGeneratorMetadata;
import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.ScoredCandidates;

public interface EquivalenceGenerator<T> {

    ScoredCandidates<T> generate(T subject, ResultDescription desc);

    default EquivalenceGeneratorMetadata getMetadata() {
        return GenericEquivalenceGeneratorMetadata.create(this.getClass().getCanonicalName());
    }
}

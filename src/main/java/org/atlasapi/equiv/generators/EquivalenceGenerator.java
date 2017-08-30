package org.atlasapi.equiv.generators;

import org.atlasapi.equiv.generators.metadata.EquivalenceGeneratorMetadata;
import org.atlasapi.equiv.generators.metadata.GenericEquivalenceGeneratorMetadata;
import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResults;

public interface EquivalenceGenerator<T> {

    ScoredCandidates<T> generate(
            T subject,
            ResultDescription desc,
            EquivToTelescopeResults equivToTelescopeResults
    );

    default EquivalenceGeneratorMetadata getMetadata() {
        return GenericEquivalenceGeneratorMetadata.create(this.getClass().getCanonicalName());
    }
}

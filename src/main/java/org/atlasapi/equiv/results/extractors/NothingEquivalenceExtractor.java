package org.atlasapi.equiv.results.extractors;

import com.google.common.collect.ImmutableSet;
import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.ScoredCandidate;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResult;
import org.atlasapi.media.entity.Content;

import java.util.List;
import java.util.Set;

public class NothingEquivalenceExtractor<T extends Content> implements EquivalenceExtractor<T> {

    @Override
    public Set<ScoredCandidate<T>> extract(
            List<ScoredCandidate<T>> equivalents,
            T sujbect,
            ResultDescription desc,
            EquivToTelescopeResult equivToTelescopeResult
    ) {
        return ImmutableSet.of();
    }

}

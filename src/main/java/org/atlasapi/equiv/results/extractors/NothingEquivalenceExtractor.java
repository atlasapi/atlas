package org.atlasapi.equiv.results.extractors;

import java.util.List;
import java.util.Set;

import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.ScoredCandidate;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResults;
import org.atlasapi.media.entity.Content;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;

public class NothingEquivalenceExtractor<T extends Content> implements EquivalenceExtractor<T> {

    @Override
    public Set<ScoredCandidate<T>> extract(
            List<ScoredCandidate<T>> equivalents,
            T sujbect,
            ResultDescription desc,
            EquivToTelescopeResults equivToTelescopeResults
    ) {
        return ImmutableSet.of();
    }

}

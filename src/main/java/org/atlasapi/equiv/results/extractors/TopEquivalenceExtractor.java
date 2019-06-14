package org.atlasapi.equiv.results.extractors;

import com.google.common.collect.ImmutableSet;
import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.ScoredCandidate;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeComponent;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResult;
import org.atlasapi.media.entity.Content;

import java.util.List;
import java.util.Set;

/**
 * Always selects a single candidate with the highest score
 */
public class TopEquivalenceExtractor<T extends Content> implements EquivalenceExtractor<T> {

    public static <T extends Content> TopEquivalenceExtractor<T> create() {
        return new TopEquivalenceExtractor<T>();
    }
    
    @Override
    public Set<ScoredCandidate<T>> extract(
            List<ScoredCandidate<T>> equivalents,
            T target,
            ResultDescription desc,
            EquivToTelescopeResult equivToTelescopeResult
    ) {
        EquivToTelescopeComponent extractorComponent = EquivToTelescopeComponent.create();
        extractorComponent.setComponentName("Top Equivalence Extractor");

        if(equivalents == null || equivalents.isEmpty()) {
            return ImmutableSet.of();
        }
        if (equivalents.get(0).candidate().getId() != null
                && equivalents.get(0).score() != null) {
            extractorComponent.addComponentResult(
                    equivalents.get(0).candidate().getId(),
                    String.valueOf(equivalents.get(0).score().asDouble())
            );
        }

        equivToTelescopeResult.addExtractorResult(extractorComponent);

        return ImmutableSet.of(equivalents.get(0));
        
    }

}

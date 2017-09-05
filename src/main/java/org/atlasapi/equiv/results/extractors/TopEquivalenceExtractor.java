package org.atlasapi.equiv.results.extractors;

import java.util.List;

import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.ScoredCandidate;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeComponent;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResults;
import org.atlasapi.media.entity.Content;

import com.google.common.base.Optional;

/**
 * Always selects the equivalent with the highest score
 */
public class TopEquivalenceExtractor<T extends Content> implements EquivalenceExtractor<T> {

    public static <T extends Content> TopEquivalenceExtractor<T> create() {
        return new TopEquivalenceExtractor<T>();
    }
    
    @Override
    public Optional<ScoredCandidate<T>> extract(
            List<ScoredCandidate<T>> equivalents,
            T target,
            ResultDescription desc,
            EquivToTelescopeResults equivToTelescopeResults
    ) {
        EquivToTelescopeComponent extractorComponent = EquivToTelescopeComponent.create();
        extractorComponent.setComponentName("Top Equivalence Extractor");

        if(equivalents == null || equivalents.isEmpty()) {
            return Optional.absent();
        }
        extractorComponent.addComponentResult(
                equivalents.get(0).candidate().getId(),
                String.valueOf(equivalents.get(0).score().asDouble())
        );

        equivToTelescopeResults.addExtractorResult(extractorComponent);

        return Optional.of(equivalents.get(0));
        
    }

}

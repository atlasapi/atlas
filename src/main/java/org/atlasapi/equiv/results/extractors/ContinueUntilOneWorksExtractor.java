package org.atlasapi.equiv.results.extractors;

import java.util.List;
import java.util.Set;

import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.ScoredCandidate;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeComponent;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResults;
import org.atlasapi.media.entity.Content;

import com.google.common.collect.ImmutableSet;

/**
 * This extractor will attempt the delegates one by one, until one of them provides at least one
 * result, or it runs out of delegates.
 */
public class ContinueUntilOneWorksExtractor<T extends Content> implements EquivalenceExtractor<T> {

    private final List<EquivalenceExtractor<T>> delegates;

    public ContinueUntilOneWorksExtractor(
            List<EquivalenceExtractor<T>> delegates) {
        this.delegates = delegates;
    }

    public static <T extends Content> ContinueUntilOneWorksExtractor<T> create(
            List<EquivalenceExtractor<T>> delegates) {
        return new ContinueUntilOneWorksExtractor<>(delegates);
    }

    @Override
    public Set<ScoredCandidate<T>> extract(
            List<ScoredCandidate<T>> candidates,
            T target,
            ResultDescription desc,
            EquivToTelescopeResults equivToTelescopeResults
    ) {
        if (candidates.isEmpty()) {
            return ImmutableSet.of();
        }

        EquivToTelescopeComponent extractorComponent = EquivToTelescopeComponent.create();
        extractorComponent.setComponentName("Continue until one works.");

        for (EquivalenceExtractor<T> delegate : delegates) {
            Set<ScoredCandidate<T>> extracted = delegate.extract(
                    candidates,
                    target,
                    desc,
                    equivToTelescopeResults
            );
            if(!extracted.isEmpty()){
                equivToTelescopeResults.addExtractorResult(extractorComponent);
                return extracted;
            }
        }

        //we wont be adding any results for presentation, we expect the underlying extractors to do that.
        equivToTelescopeResults.addExtractorResult(extractorComponent);

        return ImmutableSet.of();
    }
}
package org.atlasapi.equiv.results.extractors;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.ScoredCandidate;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeComponent;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResults;
import org.atlasapi.media.entity.Content;

import com.metabroadcast.common.stream.MoreCollectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

/**
 * This extractor will select all candidates from the first extractor, then remove them from the
 * candidates list and then pass the reduced list to the second extractor and also select all its
 * results.
 */
public class RemoveAndCombineExtractor<T extends Content> implements EquivalenceExtractor<T> {

    private final EquivalenceExtractor<T> first;
    private final EquivalenceExtractor<T> second;

    private RemoveAndCombineExtractor(EquivalenceExtractor<T> first, EquivalenceExtractor<T>  second) {
        this.first = first;
        this.second = second;
    }

    public static <T extends Content> RemoveAndCombineExtractor<T> create(EquivalenceExtractor<T>  first, EquivalenceExtractor<T>  second) {
        return new RemoveAndCombineExtractor<>(first, second);
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
        extractorComponent.setComponentName("Get the results from first extractor, then remove them from the set, then get the results from second.");

        //we wont be adding any results for presentation, we expect the underlying extractors to do that.
        equivToTelescopeResults.addExtractorResult(extractorComponent);

        Set<ScoredCandidate<T>> firstResults
                = first.extract(candidates, target, desc, equivToTelescopeResults);

        //remove the results, then get the results of the second extractor.
        ImmutableList<ScoredCandidate<T>> reducedCandidates
                = candidates.stream()
                .filter(c -> !firstResults.contains(c))
                .collect(MoreCollectors.toImmutableList());
        Set<ScoredCandidate<T>> secondResults
                = second.extract(reducedCandidates, target, desc, equivToTelescopeResults);

        return Sets.union(firstResults, secondResults);
    }
}
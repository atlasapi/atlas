package org.atlasapi.equiv.results.extractors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.stream.MoreCollectors;
import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.ScoredCandidate;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeComponent;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResult;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Publisher;

import java.util.List;
import java.util.Set;

/**
 * This extractor will filter OUT all canidates from the given list of publishers,
 * then select all candidates from the given extractor. Essentially it acts as further filtering
 * that can be applied per extractor.
 */
public class ExcludePublisherThenExtractExtractor<T extends Content> implements EquivalenceExtractor<T> {

    private final EquivalenceExtractor<T> extractor;
    private final Set<Publisher> publishers;

    private ExcludePublisherThenExtractExtractor(Set<Publisher> publishers, EquivalenceExtractor<T>  extractor) {
        this.extractor = extractor;
        this.publishers = publishers;
    }

    public static <T extends Content> ExcludePublisherThenExtractExtractor<T> create(Publisher publisher, EquivalenceExtractor<T>  extractor) {
        return new ExcludePublisherThenExtractExtractor<>(ImmutableSet.of(publisher), extractor);
    }

    public static <T extends Content> ExcludePublisherThenExtractExtractor<T> create(Set<Publisher> publishers, EquivalenceExtractor<T>  extractor) {
        return new ExcludePublisherThenExtractExtractor<>(publishers, extractor);
    }

    @Override
    public Set<ScoredCandidate<T>> extract(
            List<ScoredCandidate<T>> candidates,
            T target,
            ResultDescription desc,
            EquivToTelescopeResult equivToTelescopeResult
    ) {
        if (candidates.isEmpty()) {
            return ImmutableSet.of();
        }

        EquivToTelescopeComponent extractorComponent = EquivToTelescopeComponent.create();
        extractorComponent.setComponentName("Exclude results from " + publishers + ", then get the results from the extractor.");

        //we wont be adding any results for presentation, we expect the underlying extractors to do that.
        equivToTelescopeResult.addExtractorResult(extractorComponent);

        //remove the results, then get the results of the extractor extractor.
        ImmutableList<ScoredCandidate<T>> reducedCandidates
                = candidates.stream()
                .filter(c -> !publishers.contains(c.candidate().getPublisher()))
                .collect(MoreCollectors.toImmutableList());
        Set<ScoredCandidate<T>> results
                = extractor.extract(reducedCandidates, target, desc, equivToTelescopeResult);

        return results;
    }
}
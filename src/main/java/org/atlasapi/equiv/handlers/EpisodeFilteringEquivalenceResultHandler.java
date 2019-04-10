package org.atlasapi.equiv.handlers;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.metabroadcast.common.stream.MoreCollectors;
import org.atlasapi.equiv.ContentRef;
import org.atlasapi.equiv.EquivalenceSummary;
import org.atlasapi.equiv.EquivalenceSummaryStore;
import org.atlasapi.equiv.results.EquivalenceResult;
import org.atlasapi.equiv.results.EquivalenceResults;
import org.atlasapi.equiv.results.description.ReadableDescription;
import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.ScoredCandidate;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.ParentRef;
import org.atlasapi.media.entity.Publisher;

import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;

/**
 * Filter candidate equivalent episodes based on their parents. 
 * 
 * In strict mode an Item's candidate equivalent's Container must be
 * equivalent to the Item's Container. That is, for a candidate equivalent,
 * E, of item, I, E's Container, Ec, must be equivalent to I's Container,
 * Ic. In relaxed mode, Ec need not be equivalent to Ic but there must be
 * no other equivalent container for that source.
 * @author tom
 *
 */
public class EpisodeFilteringEquivalenceResultHandler implements EquivalenceResultHandler<Item> {

    public static EquivalenceResultHandler<Item> strict(
        EquivalenceResultHandler<Item> delegate, 
        EquivalenceSummaryStore summaryStore) {
        return new EpisodeFilteringEquivalenceResultHandler(delegate, summaryStore, true);
    }

    public static EquivalenceResultHandler<Item> relaxed(
        EquivalenceResultHandler<Item> delegate, 
        EquivalenceSummaryStore summaryStore) {
        return new EpisodeFilteringEquivalenceResultHandler(delegate, summaryStore, false);
    }
    
    private final EquivalenceResultHandler<Item> delegate;
    private final EquivalenceSummaryStore summaryStore;
    private final boolean strict;

    private EpisodeFilteringEquivalenceResultHandler(
            EquivalenceResultHandler<Item> delegate,
            EquivalenceSummaryStore summaryStore,
            boolean strict
    ) {
        this.delegate = delegate;
        this.summaryStore = summaryStore;
        this.strict = strict;
    }

    @Override
    public boolean handle(EquivalenceResults<Item> results) {
        ResultDescription desc = results.description()
            .startStage("Episode parent filter");
        
        ParentRef container = results.subject().getContainer();
        if (container == null) {
            desc.appendText("Item has no Container").finishStage();
            return delegate.handle(results);
        }
        
        String containerUri = container.getUri();
        Optional<EquivalenceSummary> possibleSummary = summaryStore
            .summariesForUris(ImmutableSet.of(containerUri))
            .get(containerUri);
        
        if (!possibleSummary.isPresent()) {
            desc.appendText("Item Container summary not found").finishStage();
            throw new ContainerSummaryRequiredException(results.subject());
        }

        EquivalenceSummary summary = possibleSummary.get();
        Multimap<Publisher,ContentRef> equivalents = summary.getEquivalents();
        List<EquivalenceResult<Item>> filteredEquivalenceResults = results.getResults().stream()
                .map(result ->
                        new EquivalenceResult<>(
                                result.subject(),
                                result.rawScores(),
                                result.combinedEquivalences(),
                                filter(result.strongEquivalences(), equivalents, desc),
                                (ReadableDescription) desc
                        )
                )
                .collect(MoreCollectors.toImmutableList());

        desc.finishStage();

        return delegate.handle(new EquivalenceResults<>(
                results.subject(),
                filteredEquivalenceResults,
                (ReadableDescription) desc
        ));
    }

    private Multimap<Publisher, ScoredCandidate<Item>> filter(
            Multimap<Publisher, ScoredCandidate<Item>> strongItems,
            Multimap<Publisher,ContentRef> containerEquivalents,
            ResultDescription desc
    ) {
        ImmutableMultimap.Builder<Publisher, ScoredCandidate<Item>> filtered =
                ImmutableMultimap.builder();
        
        for (Entry<Publisher, ScoredCandidate<Item>> scoredCandidate : strongItems.entries()) {
            Item candidate = scoredCandidate.getValue().candidate();
            
            if (filter(containerEquivalents, candidate)) {
                filtered.put(scoredCandidate);
            } else {
                desc.appendText("%s removed. Unacceptable container: %s", 
                    scoredCandidate, containerUri(candidate));
            }
        }

        return filtered.build();
    }

    private boolean filter(
            Multimap<Publisher, ContentRef> containerEquivalents,
            Item candidate
    ) {
        String candidateContainerUri = containerUri(candidate);
        if (candidateContainerUri == null) {
            return true;
        } 
        Collection<ContentRef> validContainer = containerEquivalents.get(candidate.getPublisher());
        if (validContainer.isEmpty()) {
            return !strict;
        } else if (validContainer.stream()
                .anyMatch(contentRef ->
                        contentRef.getCanonicalUri().equals(candidateContainerUri))) {
            return true;
        }
        return false;
    }

    private String containerUri(Item candidate) {
        ParentRef container = candidate.getContainer();
        return container == null
               ? null
               : candidate.getContainer().getUri();
    }
}

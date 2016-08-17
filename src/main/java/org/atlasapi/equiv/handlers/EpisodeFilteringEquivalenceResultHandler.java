package org.atlasapi.equiv.handlers;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;

import org.atlasapi.equiv.ContentRef;
import org.atlasapi.equiv.EquivalenceSummary;
import org.atlasapi.equiv.EquivalenceSummaryStore;
import org.atlasapi.equiv.results.EquivalenceResult;
import org.atlasapi.equiv.results.description.ReadableDescription;
import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.ScoredCandidate;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.ParentRef;
import org.atlasapi.media.entity.Publisher;

import com.metabroadcast.columbus.telescope.client.IngestTelescopeClientImpl;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;

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

    public static final EquivalenceResultHandler<Item> strict(
        EquivalenceResultHandler<Item> delegate, 
        EquivalenceSummaryStore summaryStore) {
        return new EpisodeFilteringEquivalenceResultHandler(delegate, summaryStore, true);
    }

    public static final EquivalenceResultHandler<Item> relaxed(
        EquivalenceResultHandler<Item> delegate, 
        EquivalenceSummaryStore summaryStore) {
        return new EpisodeFilteringEquivalenceResultHandler(delegate, summaryStore, false);
    }
    
    private final EquivalenceResultHandler<Item> delegate;
    private final EquivalenceSummaryStore summaryStore;
    private final boolean strict;

    private EpisodeFilteringEquivalenceResultHandler(EquivalenceResultHandler<Item> delegate, EquivalenceSummaryStore summaryStore, boolean strict) {
        this.delegate = delegate;
        this.summaryStore = summaryStore;
        this.strict = strict;
    }

    @Override
    public void handle(EquivalenceResult<Item> result) {

        ResultDescription desc = result.description()
            .startStage("Episode parent filter");
        
        ParentRef container = result.subject().getContainer();
        if (container == null) {
            desc.appendText("Item has no Container").finishStage();
            delegate.handle(result);
            return;
        }
        
        String containerUri = container.getUri();
        Optional<EquivalenceSummary> possibleSummary = summaryStore
            .summariesForUris(ImmutableSet.of(containerUri))
            .get(containerUri);
        
        if (!possibleSummary.isPresent()) {
            desc.appendText("Item Container summary not found").finishStage();
            return;
        }

        EquivalenceSummary summary = possibleSummary.get();
        Multimap<Publisher,ContentRef> equivalents = summary.getEquivalents();
        Multimap<Publisher, ScoredCandidate<Item>> strongEquivalences
                = filter(result.strongEquivalences(), equivalents, desc);
        
        desc.finishStage();
        delegate.handle(new EquivalenceResult<Item>(result.subject(), 
            result.rawScores(), result.combinedEquivalences(), 
            strongEquivalences, (ReadableDescription) desc));

    }

    @Override
    public void handleWithReporting(
            EquivalenceResult<Item> result,
            java.util.Optional<String> taskId,
            IngestTelescopeClientImpl telescopeClient
    ) {
        // No reporting is supported for this handler.
    }

    private Multimap<Publisher, ScoredCandidate<Item>> filter(
        Multimap<Publisher, ScoredCandidate<Item>> strongItems,
        final Multimap<Publisher,ContentRef> containerEquivalents,
        final ResultDescription desc) {
        
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

    private boolean filter(final Multimap<Publisher, ContentRef> containerEquivalents,
                              Item candidate) {
        String candidateContainerUri = containerUri(candidate);
        if (candidateContainerUri == null) {
            return true;
        } 
        Collection<ContentRef> validContainer = containerEquivalents.get(candidate.getPublisher());
        if (validContainer.isEmpty()) {
            return !strict;
        } else if (validContainer.stream().anyMatch(contentRef -> contentRef.getCanonicalUri().equals(candidateContainerUri))) {
            return true;
        }
        return false;
    }

    private String containerUri(Item candidate) {
        ParentRef container = candidate.getContainer();
        return container == null ? null 
                                 : candidate.getContainer().getUri();
    }

}

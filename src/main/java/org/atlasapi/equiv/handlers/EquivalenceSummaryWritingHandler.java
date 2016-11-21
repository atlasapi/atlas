package org.atlasapi.equiv.handlers;

import java.util.List;

import javax.annotation.Nullable;

import org.atlasapi.equiv.ContentRef;
import org.atlasapi.equiv.EquivalenceSummary;
import org.atlasapi.equiv.EquivalenceSummaryStore;
import org.atlasapi.equiv.results.EquivalenceResult;
import org.atlasapi.equiv.results.scores.ScoredCandidate;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.ParentRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Series;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

public class EquivalenceSummaryWritingHandler<T extends Content> implements EquivalenceResultHandler<T> {

    private final EquivalenceSummaryStore equivSummaryStore;

    public EquivalenceSummaryWritingHandler(EquivalenceSummaryStore equivSummaryStore) {
        this.equivSummaryStore = equivSummaryStore;
    }

    @Override
    public void handle(
            EquivalenceResult<T> result
    ) {
        equivSummaryStore.store(summaryOf(result));
    }

    private EquivalenceSummary summaryOf(EquivalenceResult<T> result) {
        String canonicalUri = result.subject().getCanonicalUri();
        String parent = parentOf(result.subject());
        List<String> candidates = candidatesFrom(result.combinedEquivalences());
        Multimap<Publisher, ContentRef> equivalents = equivalentsFrom(result.strongEquivalences());
        return new EquivalenceSummary(canonicalUri,parent,candidates,equivalents);
    }

    private String parentOf(T subject) {
        if (subject instanceof Item) {
            ParentRef container = ((Item)subject).getContainer();
            if (container != null) {
                return container.getUri();
            }
        } else if (subject instanceof Series) {
            ParentRef container = ((Series)subject).getParent();
            if (container != null) {
                return container.getUri();
            }
        }
        return null;
    }

    private List<String> candidatesFrom(ScoredCandidates<T> combinedEquivalences) {
        return ImmutableList.copyOf(Iterables.transform(combinedEquivalences.candidates().keySet(), Identified.TO_URI));
    }

    private Multimap<Publisher, ContentRef> equivalentsFrom(Multimap<Publisher, ScoredCandidate<T>> strongEquivalences) {
        return ImmutableMultimap.copyOf(Multimaps.transformValues(strongEquivalences, new Function<ScoredCandidate<T>, ContentRef>() {
            @Override
            public ContentRef apply(@Nullable ScoredCandidate<T> input) {
                return contentRefFrom(input.candidate());
            }
        }));
    }
    
    private ContentRef contentRefFrom(T candidate) {
        String canonicalUri = candidate.getCanonicalUri();
        Publisher publisher = candidate.getPublisher();
        String parent = parentOf(candidate);
        return new ContentRef(canonicalUri, publisher, parent);
    }

}

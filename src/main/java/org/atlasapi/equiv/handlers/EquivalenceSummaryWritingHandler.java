package org.atlasapi.equiv.handlers;

import com.google.common.collect.Multimap;
import com.metabroadcast.common.stream.MoreCollectors;
import org.atlasapi.equiv.ContentRef;
import org.atlasapi.equiv.EquivalenceSummary;
import org.atlasapi.equiv.EquivalenceSummaryStore;
import org.atlasapi.equiv.results.EquivalenceResult;
import org.atlasapi.equiv.results.EquivalenceResults;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.ParentRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Series;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class EquivalenceSummaryWritingHandler<T extends Content>
        implements EquivalenceResultHandler<T> {

    private final EquivalenceSummaryStore equivSummaryStore;

    public EquivalenceSummaryWritingHandler(EquivalenceSummaryStore equivSummaryStore) {
        this.equivSummaryStore = equivSummaryStore;
    }

    @Override
    public boolean handle(EquivalenceResults<T> results) {
        equivSummaryStore.store(summaryOf(results));
        return false;
    }

    private EquivalenceSummary summaryOf(EquivalenceResults<T> results) {
        String canonicalUri = results.subject().getCanonicalUri();
        String parent = parentOf(results.subject());
        Set<String> candidates = candidatesFrom(results);
        Multimap<Publisher, ContentRef> equivalents = equivalentsFrom(results);
        return new EquivalenceSummary(canonicalUri,parent,candidates,equivalents);
    }

    @Nullable
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

    private Set<String> candidatesFrom(EquivalenceResults<T> results) {
        return results.getResults().stream()
                .map(EquivalenceResult::combinedEquivalences)
                .map(ScoredCandidates::candidates)
                .map(Map::keySet)
                .flatMap(Set::stream)
                .map(Identified::getCanonicalUri)
                .collect(MoreCollectors.toImmutableSet());
    }

    private Multimap<Publisher, ContentRef> equivalentsFrom(
            EquivalenceResults<T> results
    ) {
        return results.getResults().stream()
                .map(EquivalenceResult::strongEquivalences)
                .map(Multimap::entries)
                .flatMap(Collection::stream)
                .collect(MoreCollectors.toImmutableSetMultiMap(
                        Map.Entry::getKey,
                        entry -> contentRefFrom(entry.getValue().candidate())
                ));
    }
    
    private ContentRef contentRefFrom(T candidate) {
        String canonicalUri = candidate.getCanonicalUri();
        Publisher publisher = candidate.getPublisher();
        String parent = parentOf(candidate);
        return new ContentRef(canonicalUri, publisher, parent);
    }
}

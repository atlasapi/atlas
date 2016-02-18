package org.atlasapi.equiv.generators;

import java.util.List;

import javax.annotation.Nullable;

import org.atlasapi.equiv.EquivalenceSummary;
import org.atlasapi.equiv.EquivalenceSummaryStore;
import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates.Builder;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.media.entity.ChildRef;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.ParentRef;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ResolvedContent;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.collect.OptionalMap;

/**
 * Generates equivalences for an Item based on the children of the equivalence
 * candidates of the Item's container.
 */
public class ContainerCandidatesItemEquivalenceGenerator implements EquivalenceGenerator<Item> {

    private final ContentResolver contentResolver;
    private final EquivalenceSummaryStore equivSummaryStore;
    private final Function<Container, Iterable<Item>> TO_ITEMS = new Function<Container, Iterable<Item>>() {
        @Override
        public Iterable<Item> apply(@Nullable Container input) {
            Iterable<String> childUris = Iterables.transform(input.getChildRefs(), ChildRef.TO_URI);
            ResolvedContent children = contentResolver.findByCanonicalUris(childUris);
            return Iterables.filter(children.getAllResolvedResults(), Item.class);
        }
    };

    public ContainerCandidatesItemEquivalenceGenerator(ContentResolver contentResolver, EquivalenceSummaryStore equivSummaryStore) {
        this.contentResolver = contentResolver;
        this.equivSummaryStore = equivSummaryStore;
    }

    @Override
    public ScoredCandidates<Item> generate(Item subject, ResultDescription desc) {
        Builder<Item> result = DefaultScoredCandidates.fromSource("Container");

        ParentRef parent = subject.getContainer();
        if (parent != null) {
            String parentUri = parent.getUri();
            OptionalMap<String, EquivalenceSummary> containerSummary = parentSummary(parentUri);
            Optional<EquivalenceSummary> optional = containerSummary.get(parentUri);
            if (optional.isPresent()) {
                EquivalenceSummary summary = optional.get();
                for (Item child : childrenOf(summary.getCandidates())) {
                    if (child.isActivelyPublished()) {
                        result.addEquivalent(child, Score.NULL_SCORE);
                    }
                }
            }
        }

        return result.build();
    }

    private Iterable<Item> childrenOf(ImmutableList<String> candidates) {
        List<Identified> resolvedContent = contentResolver.findByCanonicalUris(candidates).getAllResolvedResults();
        Iterable<Container> resolvedContainers = Iterables.filter(resolvedContent, Container.class);
        return Iterables.concat(Iterables.transform(resolvedContainers, TO_ITEMS));
    }

    private OptionalMap<String, EquivalenceSummary> parentSummary(String parentUri) {
        return equivSummaryStore.summariesForUris(ImmutableSet.of(parentUri));
    }
    
    @Override
    public String toString() {
        return "Container's candidates generator";
    }
}

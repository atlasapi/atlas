package org.atlasapi.equiv.generators;

import org.atlasapi.equiv.ContentRef;
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
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ResolvedContent;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicates;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multiset;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.collect.OptionalMap;

public class ContainerChildEquivalenceGenerator implements EquivalenceGenerator<Container> {
    
    public static final String NAME = "Item";

    private static final Function<ContentRef,String> TO_PARENT = new Function<ContentRef, String>() {
        @Override
        public String apply(ContentRef input) {
            return input.getParentUri();
        }
    };
    
    private final EquivalenceSummaryStore summaryStore;
    private final ContentResolver resolver;
    
    public ContainerChildEquivalenceGenerator(ContentResolver resolver, EquivalenceSummaryStore summaryStore) {
        this.resolver = resolver;
        this.summaryStore = summaryStore;
    }
    
    @Override
    public ScoredCandidates<Container> generate(Container content, ResultDescription desc) {
        OptionalMap<String,EquivalenceSummary> childSummaries = summaryStore.summariesForUris(Iterables.transform(content.getChildRefs(), ChildRef.TO_URI));
        Multiset<String> parents = HashMultiset.create();
        for (EquivalenceSummary summary : Optional.presentInstances(childSummaries.values())) {
            Iterables.addAll(parents, Iterables.filter(Iterables.transform(summary.getEquivalents().values(), TO_PARENT), Predicates.notNull()));
        }
        return scoreContainers(parents, childSummaries.size(), desc);
    }

    private ScoredCandidates<Container> scoreContainers(Multiset<String> parents, int children, ResultDescription desc) {
        Builder<Container> candidates = DefaultScoredCandidates.fromSource(NAME);

        ResolvedContent containers = resolver.findByCanonicalUris(parents.elementSet());
        
        for (Multiset.Entry<String> parent : parents.entrySet()) {
            Maybe<Identified> possibledContainer = containers.get(parent.getElement());
            if (possibledContainer.hasValue()) {
                Identified identified = possibledContainer.requireValue();
                if (identified instanceof Container) {
                    Container container = (Container) identified;
                    if (container.isActivelyPublished()) {
                        Score score = score(parent.getCount(), children);
                        candidates.addEquivalent(container, score);
                        desc.appendText(
                                "%s: scored %s (%s)",
                                container.getCanonicalUri(),
                                score,
                                container.getTitle()
                        );
                    }
                } else {
                    desc.appendText("%s: %s not container", parent, identified.getClass().getSimpleName());
                }
            } else {
                desc.appendText("%s: missing", parent);
            }
        }
        
        return candidates.build();
    }

    private Score score(int count, int children) {
        double basicScore = (double)count/children;
        return Score.valueOf(Math.min(1.0, basicScore));
    }

    @Override
    public String toString() {
        return "Container Child Result generator";
    }
}

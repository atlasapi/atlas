package org.atlasapi.equiv.generators;

import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.atlasapi.equiv.ContentRef;
import org.atlasapi.equiv.EquivalenceSummary;
import org.atlasapi.equiv.EquivalenceSummaryStore;
import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates.Builder;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeComponent;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResult;
import org.atlasapi.media.entity.ChildRef;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ResolvedContent;

import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.collect.OptionalMap;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicates;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multiset;

public class ContainerChildEquivalenceGenerator implements EquivalenceGenerator<Container> {
    
    public static final String NAME = "Container Child";

    private static final Function<ContentRef,String> TO_PARENT = new Function<ContentRef, String>() {
        @Override
        public String apply(ContentRef input) {
            return input.getParentUri();
        }
    };
    
    private final EquivalenceSummaryStore summaryStore;
    private final ContentResolver resolver;
    private final Set<Publisher> publishers;

    public ContainerChildEquivalenceGenerator(
            ContentResolver resolver,
            EquivalenceSummaryStore summaryStore
    ) {
        this(resolver, summaryStore, null);
    }

    public ContainerChildEquivalenceGenerator(
            ContentResolver resolver,
            EquivalenceSummaryStore summaryStore,
            @Nullable Set<Publisher> publishers
    ) {
        this.resolver = resolver;
        this.summaryStore = summaryStore;
        this.publishers = (publishers == null) ? null : ImmutableSet.copyOf(publishers);
    }
    
    @Override
    public ScoredCandidates<Container> generate(
            Container content,
            ResultDescription desc,
            EquivToTelescopeResult equivToTelescopeResult
    ) {

        EquivToTelescopeComponent generatorComponent = EquivToTelescopeComponent.create();
        generatorComponent.setComponentName("Container Child Equivalence Generator");

        OptionalMap<String,EquivalenceSummary> childSummaries = summaryStore.summariesForUris(
                Iterables.transform(content.getChildRefs(), ChildRef.TO_URI)
        );
        Multiset<String> parents = HashMultiset.create();
        for (EquivalenceSummary summary : Optional.presentInstances(childSummaries.values())) {
            Iterables.addAll(
                    parents,
                    Iterables.filter(
                            summary.getEquivalents()
                                    .values()
                                    .stream()
                                    .filter(input -> publishers == null
                                            || publishers.contains(input.getPublisher()))
                                    .map(TO_PARENT::apply)
                                    .collect(Collectors.toList()),
                            Predicates.notNull()
                    )
            );
        }
        return scoreContainers(
                parents,
                childSummaries.size(),
                desc,
                equivToTelescopeResult,
                generatorComponent
        );
    }

    private ScoredCandidates<Container> scoreContainers(
            Multiset<String> parents,   // parents of the children that are equiv'd to subject's children
            int children,
            ResultDescription desc,
            EquivToTelescopeResult equivToTelescopeResult,
            EquivToTelescopeComponent generatorComponent
    ) {
        Builder<Container> candidates = DefaultScoredCandidates.fromSource(NAME);

        ResolvedContent containers = resolver.findByCanonicalUris(parents.elementSet());
        
        for (Multiset.Entry<String> parent : parents.entrySet()) {
            Maybe<Identified> possibleContainer = containers.get(parent.getElement());
            if (possibleContainer.hasValue()) {
                Identified identified = possibleContainer.requireValue();
                if (identified instanceof Container) {
                    Container container = (Container) identified;
                    if (container.isActivelyPublished()) {
                        Score score = score(parent.getCount(), children);
                        candidates.addEquivalent(container, score);

                        if (container.getId() != null) {
                            generatorComponent.addComponentResult(
                                    container.getId(),
                                    String.valueOf(score.asDouble())
                            );
                        }

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

        equivToTelescopeResult.addGeneratorResult(generatorComponent);
        
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

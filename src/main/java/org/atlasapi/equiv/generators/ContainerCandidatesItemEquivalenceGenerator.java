package org.atlasapi.equiv.generators;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

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
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.ParentRef;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ResolvedContent;

import com.metabroadcast.common.collect.OptionalMap;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

/**
 * Generates equivalences for an Item based on the children of the equivalence
 * candidates of the Item's container.
 */
public class ContainerCandidatesItemEquivalenceGenerator implements EquivalenceGenerator<Item> {

    private final ContentResolver contentResolver;
    private final EquivalenceSummaryStore equivSummaryStore;

    public ContainerCandidatesItemEquivalenceGenerator(
            ContentResolver contentResolver,
            EquivalenceSummaryStore equivSummaryStore
    ) {
        this.contentResolver = contentResolver;
        this.equivSummaryStore = equivSummaryStore;
    }

    @Override
    public ScoredCandidates<Item> generate(
            Item subject,
            ResultDescription desc,
            EquivToTelescopeResult equivToTelescopeResult
    ) {
        Builder<Item> result = DefaultScoredCandidates.fromSource("Container");

        EquivToTelescopeComponent generatorComponent = EquivToTelescopeComponent.create();
        generatorComponent.setComponentName("Container Candidates Item Equivalence Generator");

        ParentRef parent = subject.getContainer();
        if (parent != null) {
            String parentUri = parent.getUri();
            OptionalMap<String, EquivalenceSummary> containerSummary = parentSummary(parentUri);
            Optional<EquivalenceSummary> optional = containerSummary.get(parentUri);

            if (optional.isPresent()) {
                EquivalenceSummary summary = optional.get();
                for (String containerUri : summary.getCandidates()) {
                    List<Identified> resolvedContent = contentResolver.findByCanonicalUris(
                            Collections.singleton(containerUri)).getAllResolvedResults();
                    Container resolvedContainer = Iterables.filter(resolvedContent, Container.class)
                            .iterator()
                            .next();

                    for (Item child : itemsOf(resolvedContainer)) {
                        //if its published, and its not the subject itself, we have a winner!
                        if (child.isActivelyPublished() &&
                                !Objects.equals(child.getId(), subject.getId())) {

                            result.addEquivalent(child, Score.NULL_SCORE);
                            desc.appendText(
                                    "Candidate: %s (from parent: %s)",
                                    child.getCanonicalUri(),
                                    resolvedContainer.getCanonicalUri()
                            );
                            generatorComponent.addComponentResult(
                                    child.getId(),
                                    ""
                            );
                        }
                    }
                }
            }
        }

        equivToTelescopeResult.addGeneratorResult(generatorComponent);

        return result.build();
    }

    private Iterable<Item> itemsOf(Container container){
        Iterable<String> childUris = Iterables.transform(container.getChildRefs(), ChildRef.TO_URI);
        ResolvedContent children = contentResolver.findByCanonicalUris(childUris);
        return Iterables.filter(children.getAllResolvedResults(), Item.class);
    }

    private OptionalMap<String, EquivalenceSummary> parentSummary(String parentUri) {
        return equivSummaryStore.summariesForUris(ImmutableSet.of(parentUri));
    }
    
    @Override
    public String toString() {
        return "Container's candidates generator";
    }
}

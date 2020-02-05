package org.atlasapi.equiv.generators;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

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
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.ParentRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ResolvedContent;

import com.metabroadcast.common.collect.OptionalMap;
import com.metabroadcast.common.stream.MoreCollectors;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.gdata.util.common.base.Nullable;

/**
 * Generates equivalences for an Item based on the children of the equivalence
 * candidates of the Item's container.
 */
public class ContainerCandidatesItemEquivalenceGenerator implements EquivalenceGenerator<Item> {

    private final ContentResolver contentResolver;
    private final EquivalenceSummaryStore equivSummaryStore;
    private final Set<Publisher> publishers;
    private final boolean strongCandidatesOnly;
    private final boolean limitToSameSeriesAndEpisodeNumber;

    public ContainerCandidatesItemEquivalenceGenerator(
            ContentResolver contentResolver,
            EquivalenceSummaryStore equivSummaryStore
    ) {
        this(contentResolver, equivSummaryStore, null, false, false);
    }

    public ContainerCandidatesItemEquivalenceGenerator(
            ContentResolver contentResolver,
            EquivalenceSummaryStore equivSummaryStore,
            @Nullable Collection<Publisher> publishers,
            boolean strongCandidatesOnly,
            boolean limitToSameSeriesAndEpisodeNumber
    ) {
        this.contentResolver = contentResolver;
        this.equivSummaryStore = equivSummaryStore;
        this.publishers = (publishers == null) ? null : ImmutableSet.copyOf(publishers);
        this.strongCandidatesOnly = strongCandidatesOnly;
        this.limitToSameSeriesAndEpisodeNumber = limitToSameSeriesAndEpisodeNumber;
    }

    public ContainerCandidatesItemEquivalenceGenerator(
            ContentResolver contentResolver,
            EquivalenceSummaryStore equivSummaryStore,
            @Nullable Set<Publisher> publishers
    ){
        this(contentResolver,equivSummaryStore, publishers, false, false);
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

        Episode epSubject = null;
        if (limitToSameSeriesAndEpisodeNumber) {
            desc.appendText("(filtered by same non_null Episode and Series number)");
            if (subject instanceof Episode) {
                epSubject = (Episode) subject;
                if (epSubject.getSeriesNumber() == null || epSubject.getEpisodeNumber() == null) {
                    desc.appendText("Series or Episode number is null, so this generator quit.");
                    return result.build();
                }
            } else {
                desc.appendText("Subject is not an episode, so this generator quit.");
                return result.build();
            }
        }


        for (String containerUri : getContainerUris(subject.getContainer())) {
            List<Identified> resolvedContent = contentResolver.findByCanonicalUris(
                    Collections.singleton(containerUri)).getAllResolvedResults();
            Container resolvedContainer = Iterables.filter(resolvedContent, Container.class)
                    .iterator()
                    .next();
            //this is necessary for when we looked at all candidates;if we looked at container's
            //equivalents only, then we already filtered out by publisher before resolving;
            if (publishers != null){
                if (!publishers.contains(resolvedContainer.getPublisher())) {
                    continue;
                }
            }
            for (Item child : itemsOf(resolvedContainer)) {
                //if its published, and its not the subject itself, we have a winner!
                if (child.isActivelyPublished() && !Objects.equals(
                        child.getId(),
                        subject.getId())) {

                    if (limitToSameSeriesAndEpisodeNumber) {
                        if( child instanceof Episode){
                            Episode epChild = (Episode) child;
                            if (epSubject.getEpisodeNumber().equals(epChild.getEpisodeNumber())
                                && epSubject.getSeriesNumber().equals(epChild.getSeriesNumber())) {

                                addResult(desc, result, generatorComponent, resolvedContainer, child);
                            }
                        }
                    } else {
                        addResult(desc, result, generatorComponent, resolvedContainer, child);
                    }
                }
            }

        }

        equivToTelescopeResult.addGeneratorResult(generatorComponent);

        return result.build();
    }

    private void addResult(
            ResultDescription desc, Builder<Item> result,
            EquivToTelescopeComponent generatorComponent,
            Container resolvedContainer,
            Item child) {

        result.addEquivalent(child, Score.NULL_SCORE);
        desc.appendText(
                "Candidate: %s (from parent: %s)",
                child.getCanonicalUri(),
                resolvedContainer.getCanonicalUri()
        );
        generatorComponent.addComponentResult(child.getId(), "");
    }

    private ImmutableList<String> getContainerUris(@Nullable ParentRef parent) {
        if (parent != null) {
            String parentUri = parent.getUri();
            OptionalMap<String, EquivalenceSummary> containerSummary = parentSummary(parentUri);
            Optional<EquivalenceSummary> optional = containerSummary.get(parentUri);

            if (optional.isPresent()) {
                EquivalenceSummary summary = optional.get();
                return fetchContainerUris(
                        summary,
                        strongCandidatesOnly,
                        publishers);
            }
        }
        return ImmutableList.of();
    }


    private Iterable<Item> itemsOf(Container container){
        Iterable<String> childUris = Iterables.transform(container.getChildRefs(), ChildRef.TO_URI);
        ResolvedContent children = contentResolver.findByCanonicalUris(childUris);
        return Iterables.filter(children.getAllResolvedResults(), Item.class);
    }

    private OptionalMap<String, EquivalenceSummary> parentSummary(String parentUri) {
        return equivSummaryStore.summariesForUris(ImmutableSet.of(parentUri));
    }

    private ImmutableList<String> fetchContainerUris(EquivalenceSummary summary,
            boolean strongCandidatesOnly, @Nullable Set<Publisher> publishers) {
        //if we are looking at equivalents only, then we can filter out by publisher before resolving
        if (strongCandidatesOnly) {
            return summary.getEquivalents().entries().stream()
                    .filter(input -> publishers == null || publishers.contains(input.getKey()))
                    .map(Map.Entry::getValue)
                    .map(ContentRef::getCanonicalUri)
                    .distinct()
                    .collect(MoreCollectors.toImmutableList());
        } else {
            return summary.getCandidates();
        }
    }

    @Override
    public String toString() {
        return "Container's candidates generator";
    }
}

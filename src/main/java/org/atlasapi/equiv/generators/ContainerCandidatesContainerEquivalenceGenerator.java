package org.atlasapi.equiv.generators;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

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
import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.ParentRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Series;
import org.atlasapi.media.entity.SeriesRef;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ResolvedContent;

import com.metabroadcast.common.collect.OptionalMap;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

/**
 * Generates equivalences for an non-top-level Container based on the children of the equivalences
 * of the Container's container.
 */
public class ContainerCandidatesContainerEquivalenceGenerator
        implements EquivalenceGenerator<Container> {

    private final ContentResolver contentResolver;
    private final EquivalenceSummaryStore equivSummaryStore;
    private final Set<Publisher> publishers;

    public ContainerCandidatesContainerEquivalenceGenerator(
            ContentResolver contentResolver,
            EquivalenceSummaryStore equivSummaryStore
    ){
        this(contentResolver,equivSummaryStore, null);
    }

    public ContainerCandidatesContainerEquivalenceGenerator(
            ContentResolver contentResolver,
            EquivalenceSummaryStore equivSummaryStore,
            @Nullable Set<Publisher> publishers
    ) {
        this.contentResolver = contentResolver;
        this.equivSummaryStore = equivSummaryStore;
        this.publishers = (publishers == null) ? null : ImmutableSet.copyOf(publishers);
    }

    @Override
    public ScoredCandidates<Container> generate(
            Container subject,
            ResultDescription desc,
            EquivToTelescopeResult equivToTelescopeResult
    ) {
        Builder<Container> result = DefaultScoredCandidates.fromSource("Container");
        EquivToTelescopeComponent generatorComponent = EquivToTelescopeComponent.create();
        generatorComponent.setComponentName("Container Candidates Container Equivalence Generator");

        if (!(subject instanceof Series)) {
            equivToTelescopeResult.addGeneratorResult(generatorComponent);
            return result.build();
        }

        Series series = (Series) subject;
        if(series.getParent() == null){
            equivToTelescopeResult.addGeneratorResult(generatorComponent);
            return result.build();
        }

        ParentRef parent = series.getParent();
        String parentUri = parent.getUri();
        OptionalMap<String, EquivalenceSummary> containerSummary = topLevelSummary(parentUri);
        Optional<EquivalenceSummary> optional = containerSummary.get(parentUri);
        if (optional.isPresent()) {
            EquivalenceSummary summary = optional.get();
            for (String containerUri : summary.getCandidates()) {
                List<Identified> resolvedContent = contentResolver.findByCanonicalUris(
                        Collections.singleton(containerUri)).getAllResolvedResults();
                Brand resolvedContainer = Iterables.filter(resolvedContent, Brand.class)
                        .iterator()
                        .next();
                if (publishers != null) {
                    if (!publishers.contains(resolvedContainer.getPublisher())) {
                        continue;
                    }
                }
                for (Series candidateSeries : seriesOf(resolvedContainer)) {
                    //if its published, and its not the subject itself, we have a winner.
                    if (candidateSeries.isActivelyPublished()
                            && !Objects.equals(candidateSeries.getId(), subject.getId())) {

                        result.addEquivalent(candidateSeries, Score.NULL_SCORE);
                        desc.appendText(
                                "Candidate: %s (from parent: %s)",
                                candidateSeries.getCanonicalUri(),
                                resolvedContainer.getCanonicalUri()
                        );
                        generatorComponent.addComponentResult(candidateSeries.getId(), "");
                    }
                }
            }
        }
        equivToTelescopeResult.addGeneratorResult(generatorComponent);

        return result.build();
    }

    private Iterable<Series> seriesOf(Brand container){
        Iterable<String> seriesUris = Iterables.transform(container.getSeriesRefs(), SeriesRef.TO_URI);
        ResolvedContent series = contentResolver.findByCanonicalUris(seriesUris);
        return Iterables.filter(series.getAllResolvedResults(), Series.class);
    }

    private OptionalMap<String, EquivalenceSummary> topLevelSummary(String parentUri) {
        return equivSummaryStore.summariesForUris(ImmutableSet.of(parentUri));
    }

    @Override
    public String toString() {
        return "Container's candidates generator";
    }
    
    private static final Function<ContentRef, String> TO_CANONICAL_URI = input-> input.getCanonicalUri() ;

    
}

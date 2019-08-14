package org.atlasapi.equiv.generators;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.collect.OptionalMap;
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

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Generates equivalences for an non-top-level Container based on the children of the equivalences
 * of the Container's container.
 */
public class ContainerCandidatesContainerEquivalenceGenerator
        implements EquivalenceGenerator<Container> {

    private final ContentResolver contentResolver;
    private final EquivalenceSummaryStore equivSummaryStore;
    private final Set<Publisher> publishers;

    private final Function<Brand, Iterable<Series>> TO_SERIES = new Function<Brand, Iterable<Series>>() {
        @Override
        public Iterable<Series> apply(@Nullable Brand input) {
            Iterable<String> seriesUris = Iterables.transform(input.getSeriesRefs(), SeriesRef.TO_URI);
            ResolvedContent series = contentResolver.findByCanonicalUris(seriesUris);
            return Iterables.filter(series.getAllResolvedResults(), Series.class);
        }
    };

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
        if (subject instanceof Series) {
            Series series = (Series) subject;
            ParentRef parent = series.getParent();
            if (parent != null) {
                String parentUri = parent.getUri();
                OptionalMap<String, EquivalenceSummary> containerSummary = topLevelSummary(parentUri);
                Optional<EquivalenceSummary> optional = containerSummary.get(parentUri);
                if (optional.isPresent()) {
                    EquivalenceSummary summary = optional.get();
                    for (Series candidateSeries : seriesOf(
                            Iterables.transform(summary.getEquivalents().values(), TO_CANONICAL_URI)
                    )) {
                        if (publishers != null){
                            if (!publishers.contains(candidateSeries.getPublisher())){
                                continue;
                            }
                        }
                        //if its published, and its not the subject itself, we have a winner.
                        if (candidateSeries.isActivelyPublished()
                            && !Objects.equals(candidateSeries.getId(), subject.getId())) {

                            result.addEquivalent(candidateSeries, Score.NULL_SCORE);

                            generatorComponent.addComponentResult(candidateSeries.getId(), "");
                        }
                    }
                }
            }
        }

        equivToTelescopeResult.addGeneratorResult(generatorComponent);

        return result.build();
    }

    private Iterable<Series> seriesOf(Iterable<String> equivalents) {
        
        List<Identified> resolvedContent = contentResolver
                .findByCanonicalUris(equivalents)
                .getAllResolvedResults();
        Iterable<Brand> resolvedContainers = Iterables.filter(resolvedContent, Brand.class);
        return Iterables.concat(Iterables.transform(resolvedContainers, TO_SERIES));
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

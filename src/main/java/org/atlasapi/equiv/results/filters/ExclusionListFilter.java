package org.atlasapi.equiv.results.filters;

import java.util.Set;

import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.ScoredCandidate;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeComponent;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResults;
import org.atlasapi.media.entity.Identified;

import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.stream.MoreCollectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;


public class ExclusionListFilter<T extends Identified> extends AbstractEquivalenceFilter<T> {

    private final Set<String> excludedUris;
    private final Set<String> excludedIds;
    private final SubstitutionTableNumberCodec codec;

    private ExclusionListFilter(Iterable<String> excludedUris, Iterable<String> excludedIds) {
        this.excludedUris = ImmutableSet.copyOf(excludedUris);
        this.excludedIds = ImmutableSet.copyOf(excludedIds);
        this.codec = SubstitutionTableNumberCodec.lowerCaseOnly();
    }

    public static <T extends Identified> ExclusionListFilter<T> create(
            Iterable<String> excludedUris,
            Iterable<String> excludedIds
    ) {
        return new ExclusionListFilter<T>(excludedUris, excludedIds);
    }

    @Override
    protected boolean doFilter(
            ScoredCandidate<T> candidate,
            T subject,
            ResultDescription desc,
            EquivToTelescopeResults equivToTelescopeResults
    ) {
        EquivToTelescopeComponent filterComponent = EquivToTelescopeComponent.create();
        filterComponent.setComponentName("Exclusion List Filter");

        ImmutableList<Long> excludedDecodedIds = excludedIds.stream()
                .map(id -> codec.decode(id).longValue())
                .collect(MoreCollectors.toImmutableList());

        boolean result = !excludedUris.contains(candidate.candidate().getCanonicalUri())
                && !excludedDecodedIds.contains(candidate.candidate().getId());

        if (!result) {
            desc.appendText("%s removed as contained in exclusion list", 
                candidate.candidate().getCanonicalUri());
            filterComponent.addComponentResult(
                    candidate.candidate().getId(),
                    "Removed as it is present in the exclusion list"
            );
        }

        equivToTelescopeResults.addFilterResult(filterComponent);

        return result;
    }
}

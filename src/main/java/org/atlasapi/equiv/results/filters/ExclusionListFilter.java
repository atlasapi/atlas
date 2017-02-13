package org.atlasapi.equiv.results.filters;

import java.util.Set;

import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.ScoredCandidate;
import org.atlasapi.media.entity.Identified;

import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.stream.MoreCollectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;


public class ExclusionListFilter<T extends Identified> extends AbstractEquivalenceFilter<T> {

    private final Set<String> excludedUris;
    private final SubstitutionTableNumberCodec codec;

    public ExclusionListFilter(Iterable<String> excludedUris) {
        this.excludedUris = ImmutableSet.copyOf(excludedUris);
        this.codec = SubstitutionTableNumberCodec.lowerCaseOnly();
    }
    
    @Override
    protected boolean doFilter(ScoredCandidate<T> candidate, T subject, ResultDescription desc) {

        ImmutableList<Long> excludedIds = excludedUris.stream()
                .filter(id -> !id.contains("http"))
                .map(id -> codec.decode(id).longValue())
                .collect(MoreCollectors.toImmutableList());

        boolean result = !excludedUris.contains(candidate.candidate().getCanonicalUri())
                && !excludedIds.contains(candidate.candidate().getId());

        if (!result) {
            desc.appendText("%s removed as contained in exclusion list", 
                candidate.candidate().getCanonicalUri());
        }

        return result;
    }

}

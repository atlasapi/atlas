package org.atlasapi.equiv.results.filters;

import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.ScoredCandidate;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeComponent;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResults;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Specialization;

import com.google.common.base.Objects;

/**
 * This filter lets the PA specialization tv items equiv to RT specialization film items otherwise,
 * it should work like SpecializationFilter does.
 */

public class RtSpecializationFilter<T extends Content> extends AbstractEquivalenceFilter<T> {

    private static final String RT_FILM_SPECIALIZATION = "film";
    private static final String PA_TV_SPECIALIZATION = "tv";

    @Override
    public boolean doFilter(
            ScoredCandidate<T> candidate,
            T subject,
            ResultDescription desc,
            EquivToTelescopeResults equivToTelescopeResults
    ) {
        EquivToTelescopeComponent filterComponent = EquivToTelescopeComponent.create();
        filterComponent.setComponentName("RT Specialization Filter");

        T equivalent = candidate.candidate();
        Specialization candSpec = equivalent.getSpecialization();
        Specialization subSpec = subject.getSpecialization();

        boolean result = candSpec == null
                || subSpec == null
                || Objects.equal(candSpec, subSpec)
                // don't filter out cases where PA's spec is TV, and RT's spec is Film
                || (Objects.equal(candSpec.toString(), RT_FILM_SPECIALIZATION)
                    && Objects.equal(subSpec.toString(), PA_TV_SPECIALIZATION))
                || (Objects.equal(candSpec.toString(), PA_TV_SPECIALIZATION)
                    && Objects.equal(subSpec.toString(), RT_FILM_SPECIALIZATION));

        if (!result) {
            desc.appendText("%s removed. %s != %s",
                    equivalent, candSpec, subSpec
            );
            filterComponent.addComponentResult(
                    candidate.candidate().getId(),
                    "Removed due to non matching specializations"
            );
        } else {
            filterComponent.addComponentResult(
                    candidate.candidate().getId(),
                    "Went through."
            );
        }

        equivToTelescopeResults.addFilterResult(filterComponent);

        return result;
    }

}

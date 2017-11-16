package org.atlasapi.equiv.results.filters;

import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.ScoredCandidate;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeComponent;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResults;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Specialization;

import com.google.common.base.Objects;

public class SpecializationFilter<T extends Content> extends AbstractEquivalenceFilter<T> {

    @Override
    public boolean doFilter(
            ScoredCandidate<T> candidate,
            T subject,
            ResultDescription desc,
            EquivToTelescopeResults equivToTelescopeResults
    ) {
        EquivToTelescopeComponent filterComponent = EquivToTelescopeComponent.create();
        filterComponent.setComponentName("Specialization Filter");

        T equivalent = candidate.candidate();
        Specialization candSpec = equivalent.getSpecialization();
        Specialization subSpec = subject.getSpecialization();
        
        boolean result = candSpec == null 
            || subSpec == null 
            || Objects.equal(candSpec, subSpec);
        
        if (!result) {
            desc.appendText("%s removed. %s != %s", 
                equivalent, candSpec, subSpec);
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

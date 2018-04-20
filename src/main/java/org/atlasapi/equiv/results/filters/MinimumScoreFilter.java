package org.atlasapi.equiv.results.filters;

import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.ScoredCandidate;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeComponent;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResults;
import org.atlasapi.media.entity.Content;

public class MinimumScoreFilter<T extends Content>  extends AbstractEquivalenceFilter<T> {

    private final double minimum;

    public MinimumScoreFilter(double minimum) {
        this.minimum = minimum;
    }
    
    public boolean doFilter(
            ScoredCandidate<T> candidate,
            T subject,
            ResultDescription desc,
            EquivToTelescopeResults equivToTelescopeResults
    ) {
        EquivToTelescopeComponent filterComponent = EquivToTelescopeComponent.create();
        filterComponent.setComponentName("Minimum Score Filter "+minimum);

        boolean result = candidate.score().isRealScore() && candidate.score().asDouble() > minimum;
        if (!result) {
            desc.appendText(
                    "removed %s (%s)",
                    candidate.candidate().getTitle(),
                    candidate.candidate().getCanonicalUri()
            );
            filterComponent.addComponentResult(
                    candidate.candidate().getId(),
                    "Removed for not reaching minimum score of " + String.valueOf(minimum)
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

package org.atlasapi.equiv.results.filters;

import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.ScoredCandidate;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeComponent;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResults;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Film;

/** Filters out films and episodes from equiving together **/
public class FilmAndEpisodeFilter<T extends Content> extends AbstractEquivalenceFilter<T> {

    @Override
    protected boolean doFilter(
            ScoredCandidate<T> input,
            T subject,
            ResultDescription desc,
            EquivToTelescopeResults equivToTelescopeResults
    ) {
        EquivToTelescopeComponent filterComponent = EquivToTelescopeComponent.create();
        filterComponent.setComponentName("Film and Episode Filter");

        if ((input.candidate() instanceof Film && subject instanceof Episode)
                || (input.candidate() instanceof Episode && subject instanceof Film)) {
            desc.appendText(
                    "%s removed since its content type does not match",
                    subject
            );
            filterComponent.addComponentResult(
                    input.candidate().getId(),
                    "Removed since its content type does not match"
            );
            equivToTelescopeResults.addFilterResult(filterComponent);
            return false;
        }

        equivToTelescopeResults.addFilterResult(filterComponent);
        return true;

    }

}

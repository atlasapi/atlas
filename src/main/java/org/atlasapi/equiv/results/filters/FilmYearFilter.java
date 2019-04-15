package org.atlasapi.equiv.results.filters;

import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.ScoredCandidate;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeComponent;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResult;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Film;

/** Filters out any films whose years differ by more than a year **/
public class FilmYearFilter<T extends Content> extends AbstractEquivalenceFilter<T> {

    private static final int NUMBER_OF_YEARS_DIFFERENT_TOLERANCE = 1;

    @Override
    protected boolean doFilter(
            ScoredCandidate<T> input,
            T subject,
            ResultDescription desc,
            EquivToTelescopeResult equivToTelescopeResult
    ) {
        EquivToTelescopeComponent filterComponent = EquivToTelescopeComponent.create();
        filterComponent.setComponentName("Film Year Filter");

        if (!(input.candidate() instanceof Film && subject instanceof Film)) {
            filterComponent.addComponentResult(
                    input.candidate().getId(),
                    "Went through, not a film."
            );
            equivToTelescopeResult.addFilterResult(filterComponent);
            return true;
        }

        Film subjectFilm = (Film) subject;
        Film candidateFilm = (Film) input.candidate();

        if (subjectFilm.getYear() == null || candidateFilm.getYear() == null) {
            desc.appendText(
                    "Subject or candidate film year is null; not applying film year filter."
            );
            filterComponent.addComponentResult(
                    input.candidate().getId(),
                    "Went through, subject or candidate film year is null; not applying film year filter."
            );
            equivToTelescopeResult.addFilterResult(filterComponent);
            return true;
        }

        int difference = Math.abs(subjectFilm.getYear() - candidateFilm.getYear());
        boolean shouldRetain = difference <= NUMBER_OF_YEARS_DIFFERENT_TOLERANCE;

        if (!shouldRetain) {
            desc.appendText(
                    "%s removed. Candidate film year of %d differs from subject of %d "
                            + "by more than %d year(s).",
                    subjectFilm,
                    candidateFilm.getYear(),
                    subjectFilm.getYear(),
                    NUMBER_OF_YEARS_DIFFERENT_TOLERANCE
            );
            if (candidateFilm.getId() != null) {
                filterComponent.addComponentResult(
                        candidateFilm.getId(),
                        "Removed as film year differs by more than "
                                + NUMBER_OF_YEARS_DIFFERENT_TOLERANCE
                );
            }
        } else {
            filterComponent.addComponentResult(
                    candidateFilm.getId(),
                    "Went through."
            );
        }

        equivToTelescopeResult.addFilterResult(filterComponent);

        return shouldRetain;
    }
}

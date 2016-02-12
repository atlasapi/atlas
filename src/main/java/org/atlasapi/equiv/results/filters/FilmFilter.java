package org.atlasapi.equiv.results.filters;

import java.util.Map;

import org.atlasapi.equiv.handlers.EquivalenceResultHandler;
import org.atlasapi.equiv.results.EquivalenceResult;
import org.atlasapi.equiv.results.description.ReadableDescription;
import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.ScoredCandidate;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Film;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;

import com.google.common.collect.ImmutableMap;

public class FilmFilter<T extends Content> extends AbstractEquivalenceFilter<T> {

    private static final int NUMBER_OF_YEARS_DIFFERENT_TOLERANCE = 1;

    @Override
    protected boolean doFilter(ScoredCandidate<T> input, T subject, ResultDescription desc) {

        if (!(input.candidate() instanceof Film && subject instanceof Film)) {
            return true;
        }

        Film subjectFilm = (Film) subject;
        Film candidateFilm = (Film) input.candidate();

        if (subjectFilm.getYear() == null || candidateFilm.getYear() == null) {
            desc.appendText("Subject or candidate film year is null; not applying film year filter.");
            return true;
        }

        int difference = Math.abs(subjectFilm.getYear() - candidateFilm.getYear());
        boolean shouldRetain = difference <= NUMBER_OF_YEARS_DIFFERENT_TOLERANCE;

        if (!shouldRetain) {
            desc.appendText("%s removed. Candidate film year of %d differs from subject of %d by more than %d years.",
                    subjectFilm, candidateFilm.getYear(), subjectFilm.getYear(),
                    NUMBER_OF_YEARS_DIFFERENT_TOLERANCE);
        }

        return shouldRetain;
    }

    @Override
    public String toString() {
        return "Film year filter";
    }

}

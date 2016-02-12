package org.atlasapi.equiv.results.filters;

import org.atlasapi.equiv.results.description.DefaultDescription;
import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidate;
import org.atlasapi.media.entity.Film;
import org.atlasapi.media.entity.Item;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FilmFilterTest {

    private final FilmFilter underTest = new FilmFilter();

    @Test
    public void testDoesntFilterNonFilms() {
        Film subject = filmWithYear(null);
        Item candidate = new Item();
        ResultDescription result = new DefaultDescription();

        assertTrue(
                underTest.doFilter(ScoredCandidate.valueOf(candidate, Score.ONE),
                        subject, result)
        );
    }

    @Test
    public void testDoesntFilterNullFilmYears() {
        Film subject = filmWithYear(null);
        Film candidate = filmWithYear(null);
        ResultDescription result = new DefaultDescription();

        assertTrue(
                underTest.doFilter(ScoredCandidate.valueOf(candidate, Score.ONE),
                        subject, result)
        );
    }

    @Test
    public void testDoesntFilterCandidateWithinTolerance() {
        Film subject = filmWithYear(2016);
        Film candidate2015 = filmWithYear(2015);
        Film candidate2017 = filmWithYear(2017);
        Film candidate2016 = filmWithYear(2016);
        ResultDescription result = new DefaultDescription();

        assertTrue(underTest.doFilter(ScoredCandidate.valueOf(candidate2015, Score.ONE),
                subject, result));
        assertTrue(underTest.doFilter(ScoredCandidate.valueOf(candidate2016, Score.ONE),
                subject, result));
        assertTrue(underTest.doFilter(ScoredCandidate.valueOf(candidate2017, Score.ONE),
                subject, result));
    }

    @Test
    public void testFiltersCandidateOutsideTolerance() {
        Film subject = filmWithYear(2016);
        Film candidate = filmWithYear(2014);
        ResultDescription result = new DefaultDescription();

        assertFalse(underTest.doFilter(ScoredCandidate.valueOf(candidate, Score.ONE),
                subject, result));

    }

    private Film filmWithYear(Integer year) {
        Film film = new Film();
        film.setYear(year);
        return film;
    }
}
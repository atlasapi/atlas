package org.atlasapi.equiv.generators;

import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.media.entity.Film;

import static com.google.common.base.Preconditions.checkNotNull;

public class FilmTitleMatcher {
    private static Score defaultScoreOnMatch = Score.ONE;
    private static Score defaultScoreOnMismatch = Score.ZERO;

    private ExpandingTitleTransformer titleExpander;

    private Score scoreOnMatch;
    private Score scoreOnMismatch;

    public FilmTitleMatcher(ExpandingTitleTransformer titleExpander) {
        this(titleExpander, defaultScoreOnMatch, defaultScoreOnMismatch);
    }

    public FilmTitleMatcher(Score scoreOnMatch, Score scoreOnMismatch) {
        this(new ExpandingTitleTransformer(), scoreOnMatch, scoreOnMismatch);
    }

    public FilmTitleMatcher(ExpandingTitleTransformer titleExpander, Score scoreOnMatch, Score scoreOnMismatch) {
        this.titleExpander = titleExpander;
        this.scoreOnMatch = checkNotNull(scoreOnMatch);
        this.scoreOnMismatch = checkNotNull(scoreOnMismatch);
    }
    
    public Score titleMatch(Film film, Film equivFilm) {
        String targetExpandedTitle = titleExpander.expand(film.getTitle());
        String candidateExpandedTitle = titleExpander.expand(equivFilm.getTitle());
        return match(removeThe(alphaNumeric(targetExpandedTitle)), removeThe(alphaNumeric(candidateExpandedTitle)));
    }

    public Score match(String subjectTitle, String equivalentTitle) {
        if(subjectTitle.length() <= equivalentTitle.length()) {
            return matchTitles(subjectTitle, equivalentTitle);
        } else {
            return matchTitles(equivalentTitle, subjectTitle);
        }
    }

    private Score matchTitles(String shorter, String longer) {
        int commonPrefix = commonPrefixLength(shorter, longer);
        
        if(commonPrefix == 0) {
            return scoreOnMismatch;
        }
        if(commonPrefix == longer.length()) {
            return scoreOnMatch;
        }

        if(!scoreOnMatch.isRealScore()) {
            return scoreOnMatch;
        }
        
        int difference = longer.length() - commonPrefix;

        double scaler = - (0.1 * scoreOnMatch.asDouble()) / Math.max(shorter.length()-1,1);

        return Score.valueOf(Math.max((difference - 1) * scaler + (0.1 * scoreOnMatch.asDouble()), 0));
    }
    
    private String removeThe(String title) {
        if(title.startsWith("the")) {
            return title.substring(3);
        }
        return title;
    }

    private String alphaNumeric(String title) {
        return title.replaceAll("[^\\d\\w]", "").toLowerCase();
    }

    private int commonPrefixLength(String t1, String t2) {
        int i = 0;
        for (; i < Math.min(t1.length(), t2.length()) && t1.charAt(i) == t2.charAt(i); i++) {
        }
        return i;
    }
}

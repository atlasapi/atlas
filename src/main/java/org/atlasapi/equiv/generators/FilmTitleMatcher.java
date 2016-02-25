package org.atlasapi.equiv.generators;

import org.atlasapi.media.entity.Film;

public class FilmTitleMatcher {

    private ExpandingTitleTransformer titleExpander;

    public FilmTitleMatcher(ExpandingTitleTransformer titleExpander) {
        this.titleExpander = titleExpander;
    }
    
    public double titleMatch(Film film, Film equivFilm) {
        String targetExpandedTitle = titleExpander.expand(film.getTitle());
        String candidateExpandedTitle = titleExpander.expand(equivFilm.getTitle());
        return match(removeThe(alphaNumeric(targetExpandedTitle)), removeThe(alphaNumeric(candidateExpandedTitle)));
    }

    public double match(String subjectTitle, String equivalentTitle) {
        if(subjectTitle.length() <= equivalentTitle.length()) {
            return matchTitles(subjectTitle, equivalentTitle);
        } else {
            return matchTitles(equivalentTitle, subjectTitle);
        }
    }

    private double matchTitles(String shorter, String longer) {
        int commonPrefix = commonPrefixLength(shorter, longer);
        
        if(commonPrefix == 0) {
            return 0.0;
        }
        if(commonPrefix == longer.length()) {
            return 1.0;
        }
        
        int difference = longer.length() - commonPrefix;

        double scaler = - 0.1 / Math.max(shorter.length()-1,1);

        return Math.max((difference - 1) * scaler + 0.1, 0);
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

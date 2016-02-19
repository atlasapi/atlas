package org.atlasapi.equiv.scorers;

import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.ImmutableSet;

import org.apache.commons.codec.language.Soundex;
import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates.Builder;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.media.entity.Item;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TitleMatchingItemScorer implements EquivalenceScorer<Item> {
    
    public static final String NAME = "Title";
    private static final ImmutableSet<String> PREFIXES = ImmutableSet.of("the ", "Live ");
    private static final Pattern TRAILING_APOSTROPHE_PATTERN =Pattern.compile("\\w' ");

    private Logger log = LoggerFactory.getLogger(TitleMatchingItemScorer.class);

    public enum TitleType {
        
        DATE("\\d{1,2}/\\d{1,2}/(\\d{2}|\\d{4})"),
        SEQUENCE("((?:E|e)pisode)(?:.*)(\\d+)"),
        DEFAULT(".*");
     
        private Pattern pattern;

        TitleType(String pattern) {
            this.pattern = Pattern.compile(pattern);
        }
        
        public static TitleType titleTypeOf(Item item) {
            return titleTypeOf(item.getTitle());
        }
        
        public static TitleType titleTypeOf(String title) {
            for (TitleType type : ImmutableList.copyOf(TitleType.values())) {
                if(type.matches(title)) {
                    return type;
                }
            }
            return DEFAULT;
        }


        private boolean matches(String title) {
            return pattern.matcher(title).matches();
        }
        
    }

    private final Score scoreOnMismatch;
    
    public TitleMatchingItemScorer() {
        this(Score.NULL_SCORE);
    }
    
    public TitleMatchingItemScorer(Score scoreOnMismatch) {
        this.scoreOnMismatch = scoreOnMismatch;
    }

    @Override
    public ScoredCandidates<Item> score(Item subject, Set<? extends Item> suggestions, ResultDescription desc) {
        Builder<Item> equivalents = DefaultScoredCandidates.fromSource(NAME);
        
        if(Strings.isNullOrEmpty(subject.getTitle())) {
            desc.appendText("No Title on subject, all score null");
        }
        
        for (Item suggestion : suggestions) {
            equivalents.addEquivalent(suggestion, score(subject, suggestion, desc));
        }
    
        return equivalents.build();
    }

    private Score score(Item subject, Item suggestion, ResultDescription desc) {
        Score score = Score.NULL_SCORE;
        if(!Strings.isNullOrEmpty(suggestion.getTitle())) {
            if(Strings.isNullOrEmpty(suggestion.getTitle())) {
                desc.appendText("No Title (%s) scored: %s", suggestion.getCanonicalUri(), score);
            } else {
                score = score(subject, suggestion);
                desc.appendText("%s (%s) scored: %s", suggestion.getTitle(), suggestion.getCanonicalUri(), score);
            }
        }
        return score;
    }


    private Score score(Item subject, Item suggestion) {
        
        TitleType subjectType = TitleType.titleTypeOf(subject.getTitle());
        TitleType suggestionType = TitleType.titleTypeOf(suggestion.getTitle());

        if (suggestion.getCanonicalUri().contains("1721364")) {
            log.debug(String.format("Title: %s, Uri: %s", suggestion.getTitle(), suggestion.getCanonicalUri()));
        }
        
        Score score = Score.NULL_SCORE;

        if(subjectType == suggestionType) {
            return compareTitles(subject.getTitle(), suggestion.getTitle());

        }
        
        return score;
    }
    
    private Score compareTitles(final String subjectTitle, final String suggestionTitle) {
        boolean matches;
        
        String subjTitle = normalize(subjectTitle);
        String suggTitle = normalize(suggestionTitle);
        
        if (appearsToBeAppreviatedWithApostrophe(subjectTitle)) {
            String regexp = normalizeRegularExpression(subjectTitle);
            matches = Pattern.matches(regexp, suggTitle);
        } else if (appearsToBeAppreviatedWithApostrophe(suggestionTitle)) {
            String regexp = normalizeRegularExpression(suggestionTitle);
            matches = Pattern.matches(regexp, subjTitle);
        } else {
            matches = subjTitle.equals(suggTitle);
        }
        
        return matches ? Score.valueOf(2D) 
                       : scoreOnMismatch;
    }
    
    private String normalize(String title) {
        return replaceSpecialChars(removeCommonPrefixes(removeSequencePrefix(title).toLowerCase()));
    }

    private String normalizeRegularExpression(String title) {
        return regularExpressionReplaceSpecialChars(removeCommonPrefixes(removeSequencePrefix(title).toLowerCase()));
    }
    private boolean appearsToBeAppreviatedWithApostrophe(String title) {
        return TRAILING_APOSTROPHE_PATTERN.matcher(title).find();
    }

    private String replaceSpecialChars(String title) {
        return title.replaceAll(" & ", " and ")
                    .replaceAll("fc ", "")
                    .replaceAll("\\s?\\/\\s?", "-") // normalize spacing around back-to-back titles
                    .replaceAll("[^A-Za-z0-9\\s']+", "-")
                    .replace(" ", "-");
                    
    }
    
    private String regularExpressionReplaceSpecialChars(String title) {
        return title.replaceAll(" & ", " and ")
                    .replaceAll("fc ", "")
                    .replaceAll("[^A-Za-z0-9\\s']+", "-")
                    .replace(" ", "\\-")
                    .replaceAll("'\\\\-", "\\\\w+\\\\-");
    }
    
    private String removeCommonPrefixes(String title) {
        String titleWithoutPrefix = title;
        for (String prefix : PREFIXES) {
            if (titleWithoutPrefix.length() > prefix.length() &&
                    titleWithoutPrefix.substring(0, prefix.length()).equalsIgnoreCase(prefix)) {
                titleWithoutPrefix = titleWithoutPrefix.substring(prefix.length());
            }
        }
        return titleWithoutPrefix;
    }

    //Matches e.g. "2. Kinross"
    private final Pattern seqTitle = Pattern.compile("\\s*\\d+\\s*[.:-]{1}\\s*(.*)");
    
    private String removeSequencePrefix(String title) {
        Matcher matcher = seqTitle.matcher(title);
        return matcher.matches() ? matcher.group(1) : title;
    }
    
    @Override
    public String toString() {
        return "Title-matching Item Scorer";
    }
}

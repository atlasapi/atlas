package org.atlasapi.equiv.scorers;

import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.ImmutableSet;
import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates.Builder;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.media.entity.Item;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;

public class TitleMatchingItemScorer implements EquivalenceScorer<Item> {
    
    public static final String NAME = "Title";
    private static final ImmutableSet<String> PREFIXES = ImmutableSet.of("the ", "Live ");

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
        
        String subjTitle = removeSequencePrefix(subject.getTitle());
        String suggTitle = removeSequencePrefix(suggestion.getTitle());
        
        subjTitle = replaceSpecialChars(removeCommonPrefixes(subjTitle.toLowerCase()));
        suggTitle = replaceSpecialChars(removeCommonPrefixes(suggTitle.toLowerCase()));
        
        Score score = Score.NULL_SCORE;

        if(subjectType == suggestionType) {
            score = subjTitle.equals(suggTitle) ? Score.ONE 
                                                : scoreOnMismatch;
        }
        
        return score;
    }

    private String replaceSpecialChars(String title) {
        return title.replaceAll(" & ", " and ")
                    .replaceAll("[^A-Za-z0-9\\s]+", "-")
                    .replace(" ", "-");
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

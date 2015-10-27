package org.atlasapi.remotesite.btvod;

import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class TitleSanitiser {

    private static final Pattern HD_SUFFIX_PATTERN = Pattern.compile("\\s\\-\\sHD");

    private static final List<Pattern> PRE_PROCESSING_PATTERNS = ImmutableList.of(
            Pattern.compile("^.* S[0-9]+[\\- ]E[0-9]+\\s?(.*)"),
            Pattern.compile("^.*Season\\s[0-9]+\\s-\\sSeason\\s[0-9]+\\s(Episode\\s[0-9]+.*)"),
            Pattern.compile("^.*?\\-\\s(.*)")
    );

    private static final Map<Pattern, String> PATTERNS_TO_REMOVE = ImmutableMap.<Pattern, String>builder()
            .put(Pattern.compile("ZQ[A-Z]{1}"), "")
            .put(Pattern.compile("_"), " ")
            .put(Pattern.compile("^Back to Backs - "), "")
            .put(Pattern.compile("\\(Curzon\\).*(-\\sHD)?$"), "")
            .put(Pattern.compile(" \\(Before DVD\\)$"), "")
            .put(Pattern.compile(" : Coming Soon$"), "")
            .put(Pattern.compile("^HD - "), "")
            .build()
    ;

    public String sanitiseTitle(String title) {
        String titleWithoutHd = stripHdPostfix(title);
        String preProcessedTitle = preProcessTitle(titleWithoutHd);
        return sanitiseTitleInternal(preProcessedTitle);
    }

    public String sanitiseSongTitle(String title) {
        String titleWithoutHd = stripHdPostfix(title);
        return sanitiseTitleInternal(titleWithoutHd);
    }

    private String stripHdPostfix(String title) {
        return HD_SUFFIX_PATTERN.matcher(title).replaceAll("");
    }

    /**
     * An episode title has usually the form of "Scrubs S4-E18 My Roommates"
     * In this case we want to extract the real episode title "My Roommates"
     * For the movie "Jurassic Park Collection - The Lost World: Jurassic Park"
     * We want to extract the title "The Lost World: Jurassic Park"
     * Otherwise we leave the title untouched
     */
    private String preProcessTitle(String title) {
        if (title == null) {
            return null;
        }

        for (Pattern titlePattern : PRE_PROCESSING_PATTERNS) {
            Matcher matcher = titlePattern.matcher(title);
            if (matcher.matches()) {
                return matcher.group(1);
            }
        }
        return title;
    }

    private String sanitiseTitleInternal(String title) {
        String sanitisedTitle = title;
        for (Map.Entry<Pattern, String> patternAndReplacement : PATTERNS_TO_REMOVE.entrySet()) {
            Pattern pattern = patternAndReplacement.getKey();
            String replacement = patternAndReplacement.getValue();
            sanitisedTitle = pattern.matcher(sanitisedTitle).replaceAll(replacement);
        }
        return sanitisedTitle.trim();
    }
}

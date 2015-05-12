package org.atlasapi.remotesite.btvod;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class TitleSanitiser {

    private static final Map<Pattern, String> PATTERNS_TO_REMOVE = ImmutableMap.<Pattern, String>builder()
            .put(Pattern.compile("ZQ[A-Z]{1}"), "")
            .put(Pattern.compile("_"), " ")
            .build()
    ;


    public String sanitiseTitle(String title) {
        String sanitisedTitle = title;
        for (Map.Entry<Pattern, String> patternAndReplacement : PATTERNS_TO_REMOVE.entrySet()) {
            Pattern pattern = patternAndReplacement.getKey();
            String replacement = patternAndReplacement.getValue();
            sanitisedTitle = pattern.matcher(sanitisedTitle).replaceAll(replacement);
        }
        return sanitisedTitle;
    }
}

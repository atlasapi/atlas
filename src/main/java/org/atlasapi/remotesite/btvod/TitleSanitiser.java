package org.atlasapi.remotesite.btvod;

import java.util.Map;
import java.util.regex.Pattern;

import com.google.common.collect.ImmutableMap;

public class TitleSanitiser {

    private static final Map<Pattern, String> PATTERNS_TO_REMOVE = ImmutableMap.<Pattern, String>builder()
            .put(Pattern.compile("ZQ[A-Z]{1}"), "")
            .put(Pattern.compile("_"), " ")
            .put(Pattern.compile("\\(Curzon\\).*(-\\sHD)?$"), "")
            .put(Pattern.compile("\\s\\-\\sHD"), "")
            .put(Pattern.compile(" \\(Before DVD\\)$"), "")
            .put(Pattern.compile(" : Coming Soon$"), "")
            .build()
    ;


    public String sanitiseTitle(String title) {
        String sanitisedTitle = title;
        for (Map.Entry<Pattern, String> patternAndReplacement : PATTERNS_TO_REMOVE.entrySet()) {
            Pattern pattern = patternAndReplacement.getKey();
            String replacement = patternAndReplacement.getValue();
            sanitisedTitle = pattern.matcher(sanitisedTitle).replaceAll(replacement);
        }
        return sanitisedTitle.trim();
    }
}

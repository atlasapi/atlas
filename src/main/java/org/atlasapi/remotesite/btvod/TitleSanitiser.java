package org.atlasapi.remotesite.btvod;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.regex.Pattern;

public class TitleSanitiser {

    private static final List<Pattern> PATTERNS_TO_REMOVE = ImmutableList.of(
            Pattern.compile("ZQ[A-Z]{1}")
    );


    public String sanitiseTitle(String title) {
        String sanitisedTitle = title;
        for (Pattern pattern : PATTERNS_TO_REMOVE) {
            sanitisedTitle = pattern.matcher(title).replaceAll("");
        }

        return sanitisedTitle;
    }
}

package org.atlasapi.remotesite.rte;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Strings;

public class RteParser {
    
    private final static String CANONICAL_URI_PREFIX = "http://rte.ie/shows/";
    private final static Pattern ID_PATTERN = Pattern.compile(".*:(\\d+)$");
    private static final String TITLE_PREFIX = "Watch ";
    private static final String TITLE_POSTFIX = " online";
    private static final String TITLE_SEASON = " Season";
    private static final String EPISODE = "Episode";
    public static final String REGEX = "\\s+Season\\s+\\d+\\s*,\\s*Episode\\s+\\d+\\s*";

    public static String canonicalUriFrom(String id) {
        checkArgument(!Strings.isNullOrEmpty(id), "Cannot build canonical uri from empty or null uri");
        
        return buildCanonicalUriFromId(id);
    }
    
    private static String buildCanonicalUriFromId(String id) {
        Matcher matcher = ID_PATTERN.matcher(id);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Failed to parse ID " + id);
        }
        return CANONICAL_URI_PREFIX + matcher.group(1);
    }

    public static String titleParser(String input) {
        String title = input;
        if (title.contains(TITLE_PREFIX)) {
            title = title.substring(
                    TITLE_PREFIX.length(),
                    title.length()
            );
        }
        if (title.contains(TITLE_POSTFIX)) {
            title = title.substring(
                    0,
                    (title.length() - TITLE_POSTFIX.length())
            );
        }

        return title.replaceFirst(REGEX, "");
    }
}

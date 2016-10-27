package org.atlasapi.remotesite.rte;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;

public class RteParser {

    private static final Logger log = LoggerFactory.getLogger(RteParser.class.getName());
    
    private final static String CANONICAL_URI_PREFIX = "http://rte.ie/shows/";
    private final static Pattern ID_PATTERN = Pattern.compile(".*:(\\d+)$");

    private static final String TITLE_PREFIX = "Watch ";

    private static final String EPISODE_TITLE_POSTFIX = " online";
    private static final String EPISODE_HIERARCHY_REGEX =
            "\\s+Season\\s+\\d+\\s*,\\s*Episode\\s+\\d+\\s*";

    private static final String FILM_TITLE_POSTFIX = " on RT&Eacute; Player";

    private RteParser() {
    }

    public static RteParser create() {
        return new RteParser();
    }

    public String canonicalUriFrom(String id) {
        checkArgument(!Strings.isNullOrEmpty(id), "Cannot build canonical uri from empty or null uri");
        
        return buildCanonicalUriFromId(id);
    }
    
    private String buildCanonicalUriFromId(String id) {
        Matcher matcher = ID_PATTERN.matcher(id);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Failed to parse ID " + id);
        }
        return CANONICAL_URI_PREFIX + matcher.group(1);
    }

    public String titleParser(String input) {
        String title = input.trim();

        if (!title.startsWith(TITLE_PREFIX)) {
            log.warn("Found title with unknown prefix. Title: {}", title);
            return title;
        }

       if (title.endsWith(FILM_TITLE_POSTFIX)) {
           return parseFilmTitle(title);
       } else if (title.endsWith(EPISODE_TITLE_POSTFIX)) {
           return parseEpisodeTitle(title);
       } else {
           log.warn("Found title with unknown postfix. Title: {}", title);
           return title;
       }
    }

    private String parseFilmTitle(String input) {
        String title = stripPrefix(input, TITLE_PREFIX);
        return stripPostfix(title, FILM_TITLE_POSTFIX);
    }

    private String parseEpisodeTitle(String input) {
        String title = stripPrefix(input, TITLE_PREFIX);
        title = stripPostfix(title, EPISODE_TITLE_POSTFIX);

        return title.replaceFirst(EPISODE_HIERARCHY_REGEX, "");
    }

    private String stripPrefix(String input, String prefix) {
        return input.substring(
                prefix.length(),
                input.length()
        );
    }

    private String stripPostfix(String title, String postfix) {
        return title.substring(
                0,
                title.length() - postfix.length()
        );
    }
}

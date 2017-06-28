package org.atlasapi.equiv.generators;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import com.metabroadcast.common.collect.ImmutableOptionalMap;
import com.metabroadcast.common.collect.OptionalMap;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

public class ExpandingTitleTransformer {

    private final OptionalMap<String, String> WORDS_TO_EXPAND =
            ImmutableOptionalMap.fromMap(ImmutableMap.<String, String>builder()
                    .put("dr", "doctor")
                    .put("1", "one")
                    .put("2", "two")
                    .put("3", "three")
                    .put("4", "four")
                    .put("5", "five")
                    .put("6", "six")
                    .put("7", "seven")
                    .put("8", "eight")
                    .put("9", "nine")
                    //replace roman numerals
                    .put("i", "one")
                    .put("ii", "two")
                    .put("iii", "three")
                    .put("iv", "four")
                    .put("v", "five")
                    .put("vi", "six")
                    .put("vii", "seven")
                    .put("viii", "eight")
                    .put("ix", "nine")
                    .put("x", "ten")
                    .put("xi", "eleven")
                    .put("xii", "twelve")
                    .put("xiii", "thirteen")
                    .put("xiv", "fourteen")
                    .put("xv", "fifteen")
                    .put("xvi", "sixteen")
                    .put("xvii", "seventeen")
                    .put("xviii", "eighteen")
                    .put("xix", "nineteen")
                    .put("xx", "twenty")
                    .build());


    public String expand(String input) {
        input = input.toLowerCase();
        List<String> words = Arrays.asList(input.split(" "));

        List<String> cleanWords = words.stream()
                .map(this::removePossesive)
                .map(this::americanize)
                .map(expander::apply).collect(Collectors.toList());

        return String.join(" ", cleanWords);
    }

    private String removePossesive(String word) {
        if (word.endsWith("'s")) {
            return replaceLast(word, "'s", "");
        }
        return word;
    }


    //This function is a stub and more rules need to be added
    private String americanize(String word) {
        if (word.endsWith("our")) {
            return replaceLast(word, "our", "or");
        }
        return word;
    }

    /**
     * Replace only the last occurrence that the regex matches.
     *
     * @param text        The string we are editing
     * @param regex       What to look for
     * @param replacement what to replace it with
     * @return the text with the last occurrence of regex replaced with replacement
     */
    public static String replaceLast(String text, String regex, String replacement) {
        return text.replaceFirst("(?s)(.*)" + regex, "$1" + replacement);
    }

    //This function now relies on receiving lowercase letters.
    private Function<String, String> expander = new Function<String, String>() {
        @Nullable
        @Override
        public String apply(@Nullable String word) {
            return WORDS_TO_EXPAND.get(word).or(word);
        }
    };
}

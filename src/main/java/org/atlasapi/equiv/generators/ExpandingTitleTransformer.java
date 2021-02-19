package org.atlasapi.equiv.generators;

import java.util.*;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import com.metabroadcast.common.collect.ImmutableOptionalMap;
import com.metabroadcast.common.collect.OptionalMap;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;

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
                    .put("center", "centre")
                    .build());


    public String expand(String input) {
        input = input.toLowerCase();
        List<String> words = Arrays.asList(input.split(" "));

        //First expand the romans at the end, then do other stuff
        //because we don't want V's to become V and then five.

        String lastWord = words.get(words.size() - 1);
        words.set(words.size() - 1, convertRomanNumerals(lastWord));

        List<String> cleanWords = words.stream()
                .map(this::americanize)
                .map(expander::apply)
                .collect(Collectors.toList());

        return String.join(" ", cleanWords);
    }

    private String convertRomanNumerals(String word) {
        Map<String, String> romanNumerals = new HashMap<>();
        romanNumerals.put("i", "one");
        romanNumerals.put("ii", "two");
        romanNumerals.put("iii", "three");
        romanNumerals.put("iv", "four");
        romanNumerals.put("v", "five");
        romanNumerals.put("vi", "six");
        romanNumerals.put("vii", "seven");
        romanNumerals.put("viii", "eight");
        romanNumerals.put("ix", "nine");
        romanNumerals.put("x", "ten");
        romanNumerals.put("xi", "eleven");
        romanNumerals.put("xii", "twelve");
        romanNumerals.put("xiii", "thirteen");
        romanNumerals.put("xiv", "fourteen");
        romanNumerals.put("xv", "fifteen");
        romanNumerals.put("xvi", "sixteen");
        romanNumerals.put("xvii", "seventeen");
        romanNumerals.put("xviii", "eighteen");
        romanNumerals.put("xix", "nineteen");
        romanNumerals.put("xx", "twenty");

        if (romanNumerals.containsKey(word)) {
            return romanNumerals.get(word);
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
     * TODO:If anyone knows a generic place to put this, move it.
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

package org.atlasapi.equiv.generators;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import com.metabroadcast.common.collect.ImmutableOptionalMap;
import com.metabroadcast.common.collect.OptionalMap;

import com.google.common.base.Function;
import com.google.common.base.Optional;
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

        //remove parts of the word we don't need
        List<String> cleanWords = new ArrayList<>();
        for (String word : words) {
            String cleanWord = clean(word);
            String britishWord = britisise(cleanWord);
            cleanWords.add(britishWord);
        }

        Iterable<String> transform = Iterables.transform(cleanWords, expander);
        return String.join(" ", transform);
    }

    /**
     * Removes bits of the words that we don't need on the expanded word
     *
     * @param word
     * @return
     */
    private String clean(String word) {
        if(word.endsWith("'s")) {
            return replaceLast(word, "'s", "");
        }
        return word;
    }

    /**
     * Convert the given word to british english.
     * <strong>This function is a stub and more rules need to be added</strong>
     * @return
     */
    private String britisise(String word){
        if(word.endsWith("our")){
            return replaceLast(word, "our", "or");
        }
        return word;
    }

    /**
     * Replace the last occurrence in a string.
     * @param text The string we are editing
     * @param regex What to look for
     * @param replacement what to replace with
     * @return the text with the last occurrence of regex replaced with replacement
     */
    public static String replaceLast(String text, String regex, String replacement) {
        return text.replaceFirst("(?s)(.*)" + regex, "$1" + replacement);
    }

    private Function<String, String> expander = new Function<String, String>() {
        @Nullable
        @Override
        public String apply(@Nullable String word) {
            return WORDS_TO_EXPAND.get(word).or(word);
        }
    };
}

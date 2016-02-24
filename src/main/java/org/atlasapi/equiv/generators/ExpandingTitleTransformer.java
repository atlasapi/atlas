package org.atlasapi.equiv.generators;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
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
                    .build());

    public String expand(String input) {
        input = input.toLowerCase();
        List<String> words = Arrays.asList(input.split(" "));
        Iterable<String> transform = Iterables.transform(words, expander);
        StringBuilder builder = new StringBuilder();
        for (String word : transform) {
            builder.append(word).append(" ");
        }
        return builder.toString().trim();
    }

    private Function<String, String> expander = new Function<String, String>() {
        @Nullable
        @Override
        public String apply(@Nullable String word) {
            return WORDS_TO_EXPAND.get(word).or(word);
        }
    };
}

package org.atlasapi.equiv.generators;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

public class TitleExpander {

    private final Map<String, String> WORDS_TO_EXPAND = ImmutableMap.<String,String>builder()
                                                        .put("dr", "doctor")
                                                        .put("3", "three")
                                                        .build();

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
            if (WORDS_TO_EXPAND.keySet().contains(word)) {
                return WORDS_TO_EXPAND.get(word);
            }
            return word;
        }
    };
}

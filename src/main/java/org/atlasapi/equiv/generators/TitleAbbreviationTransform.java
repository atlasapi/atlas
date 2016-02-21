package org.atlasapi.equiv.generators;

import java.util.Map;
import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;

public class TitleAbbreviationTransform implements Function<String, String> {

    private final Map<String, String> ABBREVIATIONS = ImmutableMap.of("dr ", "doctor ");

    @Override
    @Nullable
    public String apply(@Nullable String input) {
        if (input == null) {
            return null;
        }
        input = input.toLowerCase();

        for (String abbreviation : ABBREVIATIONS.keySet()) {
            input = input.replace(abbreviation, ABBREVIATIONS.get(abbreviation));
        }

        return input;
    }
}

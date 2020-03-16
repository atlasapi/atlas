package org.atlasapi.deer.elasticsearch.content;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.collect.Sets;
import com.metabroadcast.common.base.Maybe;

import java.util.Set;

public enum Specialization {

    TV,
    RADIO,
    MUSIC,
    FILM;
    private static final Splitter CSV_SPLITTER = Splitter.on(',').trimResults();

    public static Maybe<Specialization> fromKey(String key) {
        for (Specialization s : Specialization.values()) {
            if (key.toLowerCase().equals(s.toString())) {
                return Maybe.just(s);
            }
        }
        return Maybe.nothing();
    }

    public static Function<String, Optional<Specialization>> FROM_KEY() {
        return str -> {
            Maybe<Specialization> maybe = Specialization.fromKey(str);
            if (maybe.hasValue()) {
                return Optional.of(maybe.requireValue());
            }
            return Optional.absent();
        };
    }

    public static Iterable<Specialization> fromCsv(String csv) {
        Iterable<String> splits = CSV_SPLITTER.split(csv);
        Set<Specialization> result = Sets.newHashSet();
        for (String candidate : splits) {
            for (Specialization specialization : Specialization.values()) {
                if (candidate.toLowerCase().equals(specialization.toString())) {
                    result.add(specialization);
                    break;
                }
            }
        }
        return result;
    }

    @Override
    public String toString() {
        return name().toLowerCase();
    }
}

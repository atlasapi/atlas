package org.atlasapi.equiv.generators.barb;

import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Content;

import com.google.common.collect.ImmutableMap;

public class BroadcasterGroupTier {

    private static final Map<String, Tier> broadcasterGroupTiers = ImmutableMap.<String, Tier>builder()
            .put("1", Tier.T1)  // BBC
            .put("2", Tier.T1)  // ITV
            .put("3", Tier.T1)  // C4
            .put("4", Tier.T1)  // C5
            .put("63", Tier.T1) // UKTV
            .put("5", Tier.T2)  // SKY
            .build();

    private static final Pattern BGID_FROM_ALIAS = Pattern.compile("^gb:barb:broadcastGroup:([0-9]+):.*$");

    public enum Tier { T1, T2 }

    public static boolean hasTierOneAlias(@Nullable Content content) {

        if (content == null) {
            return false;
        }

        return content.getAliases().stream()
                .map(Alias::getNamespace)
                .filter(Objects::nonNull)
                .map(BGID_FROM_ALIAS::matcher)
                .filter(Matcher::matches)
                .map(matcher -> matcher.group(1))
                .map(broadcasterGroupTiers::get)
                .filter(Objects::nonNull)
                .anyMatch(Predicate.isEqual(Tier.T1));
    }
}

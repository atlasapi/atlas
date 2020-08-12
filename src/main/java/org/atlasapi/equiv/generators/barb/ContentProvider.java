package org.atlasapi.equiv.generators.barb;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;

import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public class ContentProvider {

    private static final Map<String, Set<String>> tierOneBroadcasters = ImmutableMap.of(
            "nitro.bbc.co.uk", ImmutableSet.of("gb:bbc:nitro:prod:version:pid", "gb:bbc:pid"),
            "cps.itv.com", ImmutableSet.of("gb:itv:production:id"),
            "pmlsd.channel4.com", ImmutableSet.of("gb:c4:episode:id"),
            "datasubmission.channel5.com", ImmutableSet.of("gb:c5:bcid"),
            "uktv.co.uk", ImmutableSet.of("gb:uktv:bcid")
    );

    public static boolean isTierOneBroadcaster(@Nonnull Item item) {
        Publisher publisher = item.getPublisher();
        if (publisher != null && tierOneBroadcasters.containsKey(publisher.key())) {
            Set<String> namespaces = tierOneBroadcasters.get(publisher.key());
            return item.getAliases().stream()
                    .map(Alias::getNamespace)
                    .filter(Objects::nonNull)
                    .map(String::trim)
                    .anyMatch(namespaces::contains);
        } else {
            return false;
        }
    }
}
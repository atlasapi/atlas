package org.atlasapi.equiv.generators.barb;

import java.util.Optional;

import javax.annotation.Nullable;

import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Publisher;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;

public class TieredBroadcaster {

    private static final String A_AND_E_BGID = "135";

    private static final BiMap<String, String> tierOneBroadcasterGroups = ImmutableBiMap.<String, String>builder()
            .put("1", "nitro.bbc.co.uk")
            .put("2", "cps.itv.com")
            .put("3", "pmlsd.channel4.com")
            .put("4", "datasubmission.channel5.com")
            .put("63", "uktv.co.uk")
            .build();

    public static final String TXLOG_BROADCASTER_GROUP = "txlog:broadcaster_group";

    public static boolean isTierOne(@Nullable Content content) {

        if (content == null) {
            return false;
        }

        Publisher publisher = content.getPublisher();
        String bgid = content.getCustomField(TXLOG_BROADCASTER_GROUP);

        return (publisher != null && tierOneBroadcasterGroups.containsValue(publisher.key()))
               || (bgid != null && tierOneBroadcasterGroups.containsKey(bgid));
    }

    public static boolean isTierOneOrAAndE(@Nullable Content content) {

        if (content == null) {
            return false;
        }

        Publisher publisher = content.getPublisher();
        String bgid = content.getCustomField(TXLOG_BROADCASTER_GROUP);

        return (publisher != null && tierOneBroadcasterGroups.containsValue(publisher.key()))
                || (bgid != null && (tierOneBroadcasterGroups.containsKey(bgid)
                    || bgid.equals(A_AND_E_BGID)));
    }

    public static Optional<String> getSource(String bgid) {
        return Optional.ofNullable(tierOneBroadcasterGroups.get(bgid));
    }

    public static Optional<String> getBroadcasterGroupId(String source) {
        return Optional.ofNullable(tierOneBroadcasterGroups.inverse().get(source));
    }
}

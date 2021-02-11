package org.atlasapi.equiv.generators.aenetworks;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Publisher;

import javax.annotation.Nullable;
import java.util.Optional;

public class TieredBroadcaster {

    private static final BiMap<String, String> aAndEBgidToPublisher = ImmutableBiMap.<String, String>builder()
            .put("135", "aenetworks.co.uk")
            .build();

    public static final String TXLOG_BROADCASTER_GROUP = "txlog:broadcaster_group";

    public static boolean isAAndE(@Nullable Content content) {

        if (content == null) {
            return false;
        }

        Publisher publisher = content.getPublisher();
        String bgid = content.getCustomField(TXLOG_BROADCASTER_GROUP);

        return (publisher != null && aAndEBgidToPublisher.containsValue(publisher.key()))
                || (bgid != null && aAndEBgidToPublisher.containsKey(bgid));
    }

    public static Optional<String> getSource(String bgid) {
        return Optional.ofNullable(aAndEBgidToPublisher.get(bgid));
    }

    public static Optional<String> getBroadcasterGroupId(String source) {
        return Optional.ofNullable(aAndEBgidToPublisher.inverse().get(source));
    }
}

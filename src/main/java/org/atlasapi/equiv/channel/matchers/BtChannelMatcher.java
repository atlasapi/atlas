package org.atlasapi.equiv.channel.matchers;

import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.entity.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BtChannelMatcher extends ChannelMatcher {

    private static final Logger log = LoggerFactory.getLogger(BtChannelMatcher.class);
    private static final SubstitutionTableNumberCodec codec = SubstitutionTableNumberCodec.lowerCaseOnly();
    private final String uriPrefix;

    private BtChannelMatcher(Publisher publisher,  Set<Publisher> allowedTargetPublishers) {
        super(publisher, allowedTargetPublishers);
        this.uriPrefix = String.format("http://%s/", publisher);
    }

    public static BtChannelMatcher create(
            Publisher publisher,
            Set<Publisher> allowedTargetPublishers) {

        return new BtChannelMatcher(publisher, allowedTargetPublishers);
    }

    @Override
    public boolean isAMatch(Channel existing, Channel candidate) {
        try {
            verifyChannelsPublishers(existing, candidate);
        } catch (IllegalArgumentException e) {
            log.error("Invalid channels passed to matcher", e);
            return false;
        }

        String baseChannelId = extractBaseIdFromUri(existing);
        long extractedBaseChannelId = codec.decode(baseChannelId).longValue();

        return Objects.equal(extractedBaseChannelId, candidate.getId());
    }

    @VisibleForTesting
    String extractBaseIdFromUri(Channel channel) {
        return channel.getUri()
                .replace(uriPrefix, "")
                .trim();
    }

}

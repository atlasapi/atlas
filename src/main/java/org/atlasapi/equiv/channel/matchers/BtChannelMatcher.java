package org.atlasapi.equiv.channel.matchers;

import com.google.common.base.Objects;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.entity.Publisher;

public class BtChannelMatcher implements ChannelMatcher {

    private static final SubstitutionTableNumberCodec codec = SubstitutionTableNumberCodec.lowerCaseOnly();
    private final String uriPrefix;

    private BtChannelMatcher(Publisher publisher) {
        uriPrefix = String.format("http://%s/", publisher);
    }

    public static BtChannelMatcher create(Publisher publisher) {
        return new BtChannelMatcher(publisher);
    }

    @Override
    public boolean isAMatch(Channel existing, Channel candidate) {

        String baseChannelId = extractBaseIdFromUri(existing);

        long extractedBaseChannelId = codec.decode(baseChannelId).longValue();

        return Objects.equal(extractedBaseChannelId, candidate.getId());
    }

    private String extractBaseIdFromUri(Channel channel) {

        return channel.getUri()
                .replace(uriPrefix, "")
                .trim();
    }

}

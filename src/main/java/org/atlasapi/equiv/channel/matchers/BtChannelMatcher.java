package org.atlasapi.equiv.channel.matchers;

import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.entity.Alias;

import java.util.Optional;

public class BtChannelMatcher implements ChannelMatcher {

    private static final String PA_CHANNEL_ID_NAMESPACE = "pa:channel:id";

    private BtChannelMatcher() {}

    public static BtChannelMatcher create() {
        return new BtChannelMatcher();
    }

    @Override
    public boolean isAMatch(Channel existing, Channel candidate) {
        Optional<Alias> existingPaAlias = findPaAlias(existing);
        Optional<Alias> candidatePaAlias = findPaAlias(candidate);

        if (existingPaAlias.isPresent() && candidatePaAlias.isPresent()) {
            Alias existingAlias = existingPaAlias.get();
            Alias candidateAlias = candidatePaAlias.get();

            return existingAlias.getValue().equals(candidateAlias.getValue());
        }

        return false;
    }

    private Optional<Alias> findPaAlias(Channel channel) {
        return channel.getAliases().stream()
                .filter(alias -> PA_CHANNEL_ID_NAMESPACE.equals(alias.getNamespace()))
                .findFirst();

    }

}

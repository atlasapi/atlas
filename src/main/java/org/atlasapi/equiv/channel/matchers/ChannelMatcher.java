package org.atlasapi.equiv.channel.matchers;

import java.util.Set;

import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.entity.Publisher;

public abstract class ChannelMatcher {

    private final Publisher publisher;
    private final Set<Publisher> allowedTargetPublishers;

    ChannelMatcher(
            Publisher publisher,
            Set<Publisher> allowedTargetPublishers) {

            this.publisher = publisher;
            this.allowedTargetPublishers = allowedTargetPublishers;
    }

    public abstract boolean isAMatch(Channel existing, Channel candidate);

    void verifyChannelsPublishers(Channel existing, Channel candidate) throws IllegalArgumentException {
        if (!existing.getSource().equals(publisher)) {
            throw new IllegalArgumentException(String.format(
                    "Existing channel publisher (%s) does not match the matchers publisher (%s)",
                    existing.getSource().key(),
                    publisher.key()
            ));
        }

        if (!allowedTargetPublishers.contains(candidate.getSource())) {
            throw new IllegalArgumentException(String.format(
                    "Existing channel publisher (%s) is only allowed to equivalate to %s. Found: %s",
                    publisher.key(),
                    allowedTargetPublishers,
                    candidate.getSource().key()
            ));
        }
    }
}

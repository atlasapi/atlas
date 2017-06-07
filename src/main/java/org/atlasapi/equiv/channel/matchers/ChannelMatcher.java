package org.atlasapi.equiv.channel.matchers;

import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.entity.Publisher;

public abstract class ChannelMatcher {

    private final Publisher publisher;

    ChannelMatcher(Publisher publisher) {
            this.publisher = publisher;
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

        if (!candidate.getSource().equals(Publisher.METABROADCAST)) {
            throw new IllegalArgumentException(String.format(
                    "Existing channel publisher (%s) is only allowed to equivalate to %s. Found: %s",
                    publisher.key(),
                    Publisher.METABROADCAST.key(),
                    candidate.getSource().key()
            ));
        }
    }
}

package org.atlasapi.equiv.channel.matchers;

import org.atlasapi.media.channel.Channel;

public interface ChannelMatcher {
        boolean isAMatch(Channel existing, Channel candidate);
}

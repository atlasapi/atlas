package org.atlasapi.equiv.channel.matchers;

import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.entity.Publisher;

import com.google.common.collect.ImmutableSet;

public class ForcedEquivChannelMatcher extends ChannelMatcher {

    private ForcedEquivChannelMatcher(Publisher publisher) {
        super(publisher, ImmutableSet.of());
    }

    public static ForcedEquivChannelMatcher create(Publisher publisher) {
        return new ForcedEquivChannelMatcher(publisher);
    }

    @Override
    public boolean isAMatch(Channel existing, Channel candidate) {
        return true; // Force equiv always matches
    }
}

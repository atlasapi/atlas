package org.atlasapi.equiv.channel;

import org.atlasapi.equiv.channel.matchers.ChannelMatcher;
import org.atlasapi.equiv.update.metadata.EquivalenceUpdaterMetadata;
import org.atlasapi.media.entity.Publisher;

import static com.google.common.base.Preconditions.checkNotNull;

public class ChannelEquivalenceUpdaterMetadata extends EquivalenceUpdaterMetadata {

    private final String matcher;
    private final String publisher;

    private ChannelEquivalenceUpdaterMetadata(String matcher, String publisher) {
        this.matcher = checkNotNull(matcher);
        this.publisher = checkNotNull(publisher);
    }

    public static ChannelEquivalenceUpdaterMetadata create(
            ChannelMatcher matcher,
            Publisher publisher
    ) {
        return new ChannelEquivalenceUpdaterMetadata(getClassName(matcher), publisher.key());
    }

}

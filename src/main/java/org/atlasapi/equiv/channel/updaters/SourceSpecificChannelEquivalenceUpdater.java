package org.atlasapi.equiv.channel.updaters;

import org.atlasapi.equiv.ChannelRef;
import org.atlasapi.equiv.channel.ChannelEquivalenceUpdaterMetadata;
import org.atlasapi.equiv.channel.matchers.ChannelMatcher;
import org.atlasapi.equiv.update.EquivalenceUpdater;
import org.atlasapi.equiv.update.metadata.EquivalenceUpdaterMetadata;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelQuery;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.channel.ChannelWriter;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.reporting.telescope.OwlTelescopeReporter;
import org.atlasapi.reporting.telescope.TelescopeUtilityMethodsAtlas;

import java.util.Optional;
import java.util.Set;
import java.util.stream.StreamSupport;

import static com.google.common.base.Preconditions.checkNotNull;

public class SourceSpecificChannelEquivalenceUpdater implements EquivalenceUpdater<Channel> {

    private final Publisher publisher;
    private final ChannelMatcher channelMatcher;
    private final ChannelWriter channelWriter;
    private final ChannelResolver channelResolver;
    private final EquivalenceUpdaterMetadata metadata;

    private SourceSpecificChannelEquivalenceUpdater(Builder builder) {
        this.publisher = checkNotNull(builder.publisher);
        this.channelMatcher = checkNotNull(builder.channelMatcher);
        this.channelWriter = checkNotNull(builder.channelWriter);
        this.channelResolver = checkNotNull(builder.channelResolver);
        this.metadata = checkNotNull(builder.metadata);
    }

    @Override
    public boolean updateEquivalences(Channel subject, OwlTelescopeReporter telescopeProxy) {
        verify(subject, publisher);

        Optional<Channel> potentialCandidate = StreamSupport.stream(
                channelResolver.allChannels(
                        ChannelQuery.builder()
                                .withPublisher(Publisher.METABROADCAST)
                                .build()).spliterator(),
                false
        )
                .filter(candidate -> channelMatcher.isAMatch(subject, candidate))
                .findFirst();

        potentialCandidate.ifPresent(candidate ->
                setAndUpdateEquivalents(candidate, subject, telescopeProxy)
        );

        return true;
    }

    private void setAndUpdateEquivalents(
            Channel candidate,
            Channel subject,
            OwlTelescopeReporter telescopeProxy
    ) {

        ChannelRef subjectRef = subject.toChannelRef();
        ChannelRef candidateRef = candidate.toChannelRef();

        if (!candidate.getSameAs().contains(subjectRef)) {
            candidate.addSameAs(subjectRef);
            channelWriter.createOrUpdate(candidate);
        }

        if (!subject.getSameAs().contains(candidateRef)) {
            subject.addSameAs(candidateRef);
            channelWriter.createOrUpdate(subject);
        }

        telescopeProxy.reportSuccessfulEvent(
                subject.getId(),
                subject.getAliases(),
                subject
        );

    }

    private void verify(Channel channel, Publisher publisher) {
        if (!channel.getSource().equals(publisher)) {
            throw new IllegalArgumentException("Channel source does not match updater source");
        }
    }

    @Override
    public EquivalenceUpdaterMetadata getMetadata(Set<Publisher> sources) {
        return metadata;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private Publisher publisher;
        private ChannelMatcher channelMatcher;
        private ChannelResolver channelResolver;
        private ChannelWriter channelWriter;
        private ChannelEquivalenceUpdaterMetadata metadata;

        private Builder() {

        }

        public Builder forPublisher(Publisher publisher) {
            this.publisher = publisher;
            return this;
        }

        public Builder withChannelMatcher(ChannelMatcher channelMatcher) {
            this.channelMatcher = channelMatcher;
            return this;
        }

        public Builder withChannelResolver(ChannelResolver channelResolver) {
            this.channelResolver = channelResolver;
            return this;
        }

        public Builder withChannelWriter(ChannelWriter channelWriter) {
            this.channelWriter = channelWriter;
            return this;
        }

        public Builder withMetadata(ChannelEquivalenceUpdaterMetadata metadata) {
            this.metadata = metadata;
            return this;
        }

        public EquivalenceUpdater<Channel> build() {
            return new SourceSpecificChannelEquivalenceUpdater(this);
        }
    }
}

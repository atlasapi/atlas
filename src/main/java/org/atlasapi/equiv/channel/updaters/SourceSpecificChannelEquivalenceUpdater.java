package org.atlasapi.equiv.channel.updaters;

import java.util.Optional;
import java.util.Set;
import java.util.stream.StreamSupport;

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

import com.metabroadcast.columbus.telescope.client.EntityType;

import static com.google.common.base.Preconditions.checkNotNull;

public class SourceSpecificChannelEquivalenceUpdater implements EquivalenceUpdater<Channel> {

    private final Publisher publisher;
    private final ChannelMatcher channelMatcher;
    private final ChannelWriter channelWriter;
    private final ChannelResolver channelResolver;
    private final EquivalenceUpdaterMetadata metadata;
    private final Set<Publisher> candidateSources;

    SourceSpecificChannelEquivalenceUpdater(Builder builder) {
        this.publisher = checkNotNull(builder.publisher);
        this.channelMatcher = checkNotNull(builder.channelMatcher);
        this.candidateSources = checkNotNull(builder.candidateSources);
        this.channelWriter = checkNotNull(builder.channelWriter);
        this.channelResolver = checkNotNull(builder.channelResolver);
        this.metadata = checkNotNull(builder.metadata);
    }

    @Override
    public boolean updateEquivalences(Channel subject, OwlTelescopeReporter telescope) {
        verify(subject, publisher);

        Optional<Channel> potentialCandidate =
                candidateSources.stream()
                        .map(source ->
                                channelResolver.allChannels(ChannelQuery.builder()
                                        .withPublisher(source)
                                        .build()))
                        .flatMap(i -> StreamSupport.stream(i.spliterator(), false))
                        .filter(candidate -> channelMatcher.isAMatch(subject, candidate))
                        .findFirst();

        potentialCandidate.ifPresent(candidate ->
                setAndUpdateEquivalents(candidate, subject, telescope)
        );

        return true;
    }

    void setAndUpdateEquivalents(
            Channel candidate,
            Channel subject,
            OwlTelescopeReporter telescope
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

        telescope.reportSuccessfulEvent(
                subject.getId(),
                subject.getAliases(),
                EntityType.CHANNEL,
                subject
        );

        telescope.reportSuccessfulEvent(
                candidate.getId(),
                candidate.getAliases(),
                EntityType.CHANNEL,
                candidate
        );
    }

    void verify(Channel channel, Publisher publisher) {
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
        private Set<Publisher> candidateSources;

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

        public Builder withCandidateSources(Set<Publisher> candidateSources) {
            this.candidateSources = candidateSources;
            return this;
        }

        public EquivalenceUpdater<Channel> build() {
            return new SourceSpecificChannelEquivalenceUpdater(this);
        }
    }
}

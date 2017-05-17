package org.atlasapi.equiv.channel;

import com.metabroadcast.common.scheduling.ScheduledTask;
import org.atlasapi.equiv.channel.matchers.ChannelMatcher;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelQuery;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.channel.ChannelWriter;
import org.atlasapi.media.entity.Publisher;

import java.util.Optional;
import java.util.stream.StreamSupport;

import static com.google.common.base.Preconditions.checkNotNull;

public class ChannelEquivalenceUpdateTask extends ScheduledTask {

    private final Publisher publisher;
    private final ChannelMatcher channelMatcher;
    private final ChannelWriter channelWriter;
    private final ChannelResolver channelResolver;

    private ChannelEquivalenceUpdateTask(Builder builder) {
        this.publisher = checkNotNull(builder.publisher);
        this.channelMatcher = checkNotNull(builder.channelMatcher);
        this.channelWriter = checkNotNull(builder.channelWriter);
        this.channelResolver = checkNotNull(builder.channelResolver);
    }

    @Override
    public void runTask() {

        Iterable<Channel> publishersChannels = channelResolver.allChannels(
                ChannelQuery.builder().withPublisher(publisher).build()
        );

        publishersChannels.forEach(this::processChannel);

    }

    private void processChannel(Channel subject) {
        Optional<Channel> potentialCandidate = StreamSupport.stream(
                channelResolver.allChannels(
                        ChannelQuery.builder()
                        .withPublisher(Publisher.METABROADCAST)
                        .build()).spliterator(),
                false
        )
                .filter(candidate -> channelMatcher.isAMatch(candidate, subject))
                .findFirst();

        potentialCandidate.ifPresent(candidate ->
                setAndUpdateEquivalents(candidate, subject)
        );

    }

    private void setAndUpdateEquivalents(Channel candidate, Channel subject) {

        candidate.addSameAs(subject.toChannelRef());

        channelWriter.createOrUpdate(candidate);
    }

    public static Builder builder(String name) {
        return new Builder(name);
    }

    public static class Builder {
        private String name;
        private Publisher publisher;
        private ChannelMatcher channelMatcher;
        private ChannelResolver channelResolver;
        private ChannelWriter channelWriter;

        private Builder(String name) {
            this.name = name;
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

        public ScheduledTask build() {
            return new ChannelEquivalenceUpdateTask(this).withName(name);
        }
    }

}

package org.atlasapi.output;

import com.metabroadcast.applications.client.model.internal.Application;
import com.metabroadcast.common.stream.MoreCollectors;
import org.atlasapi.equiv.ChannelRef;
import org.atlasapi.equiv.OutputChannelMerger;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.entity.simple.ChannelQueryResult;

import java.util.Set;
import java.util.stream.StreamSupport;

public class MergingChannelModelWriter
        extends TransformingModelWriter<Iterable<Channel>, ChannelQueryResult> {

    private final TransformingModelWriter<Iterable<Channel>, ChannelQueryResult> delegate;
    private final OutputChannelMerger merger;
    private final ChannelResolver channelResolver;

    private MergingChannelModelWriter(Builder builder) {
        super(builder.queryResultModelWriter);
        this.delegate = builder.delegate;
        this.merger = builder.merger;
        this.channelResolver = builder.channelResolver;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    protected ChannelQueryResult transform(
            Iterable<Channel> channels,
            final Set<Annotation> annotations,
            final Application application
    ) {

        Iterable<Channel> mergedChannels = StreamSupport.stream(channels.spliterator(), false)
                .collect(MoreCollectors.toImmutableMap(
                        channel -> channel,
                        channel -> channelResolver.forIds(
                                channel.getSameAs().stream()
                                        .map(ChannelRef::getId)
                                        .collect(MoreCollectors.toImmutableSet()))
                ))
                .entrySet().stream()
                .map(entry -> merger.merge(application, entry.getKey(), entry.getValue()))
                .collect(MoreCollectors.toImmutableList());

        return delegate.transform(mergedChannels, annotations, application);
    }

    public static final class Builder {

        private AtlasModelWriter<ChannelQueryResult> queryResultModelWriter;
        private TransformingModelWriter<Iterable<Channel>, ChannelQueryResult> delegate;
        private OutputChannelMerger merger;
        private ChannelResolver channelResolver;

        private Builder() {
        }

        public Builder withQueryResultModelWriter(AtlasModelWriter<ChannelQueryResult> queryResultModelWriter) {
            this.queryResultModelWriter = queryResultModelWriter;
            return this;
        }

        public Builder withDelegate(
                TransformingModelWriter<Iterable<Channel>, ChannelQueryResult> delegate) {
            this.delegate = delegate;
            return this;
        }

        public Builder withMerger(OutputChannelMerger merger) {
            this.merger = merger;
            return this;
        }

        public Builder withChannelResolver(ChannelResolver channelResolver) {
            this.channelResolver = channelResolver;
            return this;
        }

        public MergingChannelModelWriter build() {
            return new MergingChannelModelWriter(this);
        }
    }
}

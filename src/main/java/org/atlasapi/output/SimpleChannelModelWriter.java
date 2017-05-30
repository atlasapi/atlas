package org.atlasapi.output;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.atlasapi.equiv.OutputChannelMerger;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.entity.simple.ChannelQueryResult;
import org.atlasapi.output.simple.ChannelModelSimplifier;

import com.metabroadcast.applications.client.model.internal.Application;
import com.metabroadcast.common.stream.MoreCollectors;

/**
 * {@link AtlasModelWriter} that translates the full Atlas Channel model
 * into a simplified form and renders that as XML.
 *
 * @author Oliver Hall (oli@metabroadcast.com)
 */
public class SimpleChannelModelWriter
        extends TransformingModelWriter<Iterable<Channel>, ChannelQueryResult> {

    private final ChannelModelSimplifier simplifier;

    public SimpleChannelModelWriter(
            AtlasModelWriter<ChannelQueryResult> delegate,
            ChannelModelSimplifier simplifier
    ) {
        super(delegate);
        this.simplifier = simplifier;
    }

    @Override
    protected ChannelQueryResult transform(
            Iterable<Channel> channels,
            final Set<Annotation> annotations,
            final Application application
    ) {
        ChannelQueryResult simpleChannels = new ChannelQueryResult();

        simpleChannels.setChannels(StreamSupport.stream(
                channels.spliterator(), false)
                .map(input -> simplifier.simplify(input, annotations, application))
                .collect(MoreCollectors.toImmutableList()));

        return simpleChannels;
    }

}

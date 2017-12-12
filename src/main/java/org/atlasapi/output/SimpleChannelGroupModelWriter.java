package org.atlasapi.output;

import com.google.common.collect.Iterables;
import com.metabroadcast.applications.client.model.internal.Application;
import org.atlasapi.media.channel.ChannelGroup;
import org.atlasapi.media.entity.simple.ChannelGroupQueryResult;
import org.atlasapi.output.simple.ChannelGroupModelSimplifier;

import java.util.Set;

/**
 * {@link AtlasModelWriter} that translates the full Atlas ChannelGroup model
 * into a simplified form and renders that as XML.
 *  
 * @author Oliver Hall (oli@metabroadcast.com)
 */
public class SimpleChannelGroupModelWriter extends TransformingModelWriter<Iterable<ChannelGroup>, ChannelGroupQueryResult> {

    private final ChannelGroupModelSimplifier simplifier;

    public SimpleChannelGroupModelWriter(
            AtlasModelWriter<ChannelGroupQueryResult> outputter,
            ChannelGroupModelSimplifier simplifier
    ) {
        super(outputter);
        this.simplifier = simplifier;
    }
    
    @Override
    protected ChannelGroupQueryResult transform(
            Iterable<ChannelGroup> channelGroups,
            final Set<Annotation> annotations,
            final Application application
    ) {
        ChannelGroupQueryResult simpleChannelGroups = new ChannelGroupQueryResult();
        simpleChannelGroups.setChannelGroups(Iterables.transform(channelGroups,
                input -> simplifier.simplify(input, annotations, application)));
        return simpleChannelGroups;
    }

}

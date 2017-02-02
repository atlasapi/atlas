package org.atlasapi.output;

import java.util.Set;

import org.atlasapi.application.v3.ApplicationConfiguration;
import org.atlasapi.media.channel.ChannelGroup;
import org.atlasapi.media.entity.simple.ChannelGroupQueryResult;
import org.atlasapi.output.simple.ChannelGroupModelSimplifier;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;

/**
 * {@link AtlasModelWriter} that translates the full Atlas ChannelGroup model
 * into a simplified form and renders that as XML.
 *  
 * @author Oliver Hall (oli@metabroadcast.com)
 */
public class SimpleChannelGroupModelWriter extends TransformingModelWriter<Iterable<ChannelGroup>, ChannelGroupQueryResult> {

    private final ChannelGroupModelSimplifier simplifier;

    public SimpleChannelGroupModelWriter(AtlasModelWriter<ChannelGroupQueryResult> outputter, ChannelGroupModelSimplifier simplifier) {
        super(outputter);
        this.simplifier = simplifier;
    }
    
    @Override
    protected ChannelGroupQueryResult transform(Iterable<ChannelGroup> channelGroups, final Set<Annotation> annotations,
            final ApplicationConfiguration config) {
        ChannelGroupQueryResult simpleChannelGroups = new ChannelGroupQueryResult();
        simpleChannelGroups.setChannelGroups(Iterables.transform(channelGroups, new Function<ChannelGroup, org.atlasapi.media.entity.simple.ChannelGroup>() {

            @Override
            public org.atlasapi.media.entity.simple.ChannelGroup apply(ChannelGroup input) {
                return simplifier.simplify(input, annotations, config);
            }
        }));
        return simpleChannelGroups;
    }

}

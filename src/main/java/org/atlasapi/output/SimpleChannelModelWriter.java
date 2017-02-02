package org.atlasapi.output;

import java.util.Set;

import org.atlasapi.application.v3.ApplicationConfiguration;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.entity.simple.ChannelQueryResult;
import org.atlasapi.output.simple.ChannelModelSimplifier;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;

/**
 * {@link AtlasModelWriter} that translates the full Atlas Channel model
 * into a simplified form and renders that as XML.
 *  
 * @author Oliver Hall (oli@metabroadcast.com)
 */
public class SimpleChannelModelWriter extends TransformingModelWriter<Iterable<Channel>, ChannelQueryResult> {

    private final ChannelModelSimplifier simplifier;

    public SimpleChannelModelWriter(AtlasModelWriter<ChannelQueryResult> delegate, ChannelModelSimplifier simplifier) {
        super(delegate);
        this.simplifier = simplifier;
    }
    
    @Override
    protected ChannelQueryResult transform(Iterable<Channel> channels, final Set<Annotation> annotations, final ApplicationConfiguration config) {
        ChannelQueryResult simpleChannels = new ChannelQueryResult();
        simpleChannels.setChannels(Iterables.transform(channels, new Function<Channel, org.atlasapi.media.entity.simple.Channel>() {

            @Override
            public org.atlasapi.media.entity.simple.Channel apply(Channel input) {
                return simplifier.simplify(input, annotations, config);
            }
        }));
        return simpleChannels;
    }

}

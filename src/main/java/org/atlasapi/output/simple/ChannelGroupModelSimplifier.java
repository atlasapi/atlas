package org.atlasapi.output.simple;

import java.util.Set;

import org.atlasapi.application.v3.ApplicationConfiguration;
import org.atlasapi.media.channel.ChannelGroup;
import org.atlasapi.output.Annotation;

public class ChannelGroupModelSimplifier extends IdentifiedModelSimplifier<ChannelGroup, org.atlasapi.media.entity.simple.ChannelGroup> {
    
    private final ChannelGroupSimplifier simplifier;
    private final ChannelNumberingsChannelGroupToChannelModelSimplifier numberingSimplifier;

    public ChannelGroupModelSimplifier(ChannelGroupSimplifier simplifier, ChannelNumberingsChannelGroupToChannelModelSimplifier numberingSimplifier) {
        this.simplifier = simplifier;
        this.numberingSimplifier = numberingSimplifier;
    }
    
    @Override
    public org.atlasapi.media.entity.simple.ChannelGroup simplify(ChannelGroup input, Set<Annotation> annotations,
            ApplicationConfiguration config) {
        org.atlasapi.media.entity.simple.ChannelGroup simple = simplifier.simplify(input, annotations.contains(Annotation.HISTORY));

        if(annotations.contains(Annotation.CHANNELS)) {
            simple.setChannels(numberingSimplifier.simplify(input.getChannelNumberings(), annotations, config));
        }
        
        return simple;
    }
}

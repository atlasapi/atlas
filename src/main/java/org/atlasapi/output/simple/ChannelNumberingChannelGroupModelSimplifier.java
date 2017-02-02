package org.atlasapi.output.simple;

import java.util.Set;

import org.atlasapi.application.v3.ApplicationConfiguration;
import org.atlasapi.media.channel.ChannelGroup;
import org.atlasapi.output.Annotation;

public class ChannelNumberingChannelGroupModelSimplifier extends IdentifiedModelSimplifier<ChannelGroup, org.atlasapi.media.entity.simple.ChannelGroup> {

    private final ChannelGroupSimplifier simplifier;

    public ChannelNumberingChannelGroupModelSimplifier(ChannelGroupSimplifier simplifier) {
        this.simplifier = simplifier;
    }
    
    @Override
    public org.atlasapi.media.entity.simple.ChannelGroup simplify(ChannelGroup input, Set<Annotation> annotations,
            ApplicationConfiguration config) {
        return simplifier.simplify(input, annotations.contains(Annotation.HISTORY));
    }
}

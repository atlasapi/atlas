package org.atlasapi.output.simple;

import java.util.Set;

import org.atlasapi.application.v3.ApplicationConfiguration;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.output.Annotation;

public class ChannelModelSimplifier extends IdentifiedModelSimplifier<Channel, org.atlasapi.media.entity.simple.Channel> {

    private final ChannelSimplifier simplifier;
    private final ChannelNumberingsChannelToChannelGroupModelSimplifier numberingSimplifier;

    public ChannelModelSimplifier(ChannelSimplifier simplifier, ChannelNumberingsChannelToChannelGroupModelSimplifier numberingSimplifier) {
        this.simplifier = simplifier;
        this.numberingSimplifier = numberingSimplifier;
    }
    
    @Override
    public org.atlasapi.media.entity.simple.Channel simplify(Channel input, final Set<Annotation> annotations,
            final ApplicationConfiguration config) {
        org.atlasapi.media.entity.simple.Channel simple = simplifier.simplify(
            input, 
            annotations.contains(Annotation.HISTORY), 
            annotations.contains(Annotation.PARENT), 
            annotations.contains(Annotation.VARIATIONS),
            annotations.contains(Annotation.CHANNEL_GROUPS_SUMMARY),
            config
        );
        
        if(annotations.contains(Annotation.CHANNEL_GROUPS)) {
            simple.setChannelGroups(numberingSimplifier.simplify(input.getChannelNumbers(), annotations, config));
        }

        return simple;
    }
}

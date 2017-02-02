package org.atlasapi.output.simple;

import java.util.Set;

import com.metabroadcast.applications.client.model.internal.Application;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.output.Annotation;

public class ChannelNumberingChannelModelSimplifier extends IdentifiedModelSimplifier<Channel, org.atlasapi.media.entity.simple.Channel> {

    private final ChannelSimplifier simplifier;
    
    public ChannelNumberingChannelModelSimplifier(ChannelSimplifier simplifier) {
        this.simplifier = simplifier;
    }
    
    @Override
    public org.atlasapi.media.entity.simple.Channel simplify(
            Channel input,
            final Set<Annotation> annotations,
            final Application application
    ) {
        return simplifier.simplify(
            input, 
            annotations.contains(Annotation.HISTORY), 
            annotations.contains(Annotation.PARENT), 
            annotations.contains(Annotation.VARIATIONS),
            annotations.contains(Annotation.CHANNEL_GROUPS_SUMMARY),
            application
        );
    }
}

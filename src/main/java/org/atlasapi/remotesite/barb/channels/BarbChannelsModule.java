package org.atlasapi.remotesite.barb.channels;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import org.atlasapi.input.ChannelModelTransformer;
import org.atlasapi.input.ImageModelTranslator;
import org.atlasapi.media.channel.ChannelWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class BarbChannelsModule {

    private @Autowired ChannelWriter channelWriter;
    private final ObjectMapper mapper = new ObjectMapper().findAndRegisterModules().registerModule(new Jdk8Module());
    private final ChannelModelTransformer modelTransformer;

    public BarbChannelsModule() {
        modelTransformer = ChannelModelTransformer.create(
                SubstitutionTableNumberCodec.lowerCaseOnly(),
                ImageModelTranslator.create()
        );
    }

    @Bean
    protected BarbChannelForceIngestController barbChannelForceIngestController() {
        return new BarbChannelForceIngestController(channelWriter, modelTransformer, mapper);
    }

}

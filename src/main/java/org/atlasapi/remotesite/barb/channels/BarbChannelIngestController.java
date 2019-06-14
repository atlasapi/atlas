package org.atlasapi.remotesite.barb.channels;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.util.Lists;
import com.google.common.collect.Maps;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import org.atlasapi.input.ChannelModelTransformer;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelWriter;
import org.atlasapi.remotesite.barb.channels.response.BarbChannelIngestResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Context;
import java.io.IOException;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;

@Controller
public class BarbChannelIngestController {

    private static final Logger log = LoggerFactory.getLogger(BarbChannelIngestController.class);

    private final ChannelWriter channelWriter;
    private final ObjectMapper mapper;
    private final SubstitutionTableNumberCodec codec = SubstitutionTableNumberCodec.lowerCaseOnly();

    private final BarbStringToChannelTransformer barbChannelTransformer;

    public BarbChannelIngestController(
            ChannelWriter channelWriter,
            ChannelModelTransformer modelTransformer,
            ObjectMapper mapper
    ) {
        this.channelWriter = checkNotNull(channelWriter);
        this.mapper = checkNotNull(mapper);

        this.barbChannelTransformer = BarbStringToChannelTransformer.create(modelTransformer);
    }

    /**
     * Channel ingest strings should separate each channel with a | and then comma separated station
     * code and channel name. The channel name should not contain any commas.
     *
     * @param channels code,name|code,name|code,name...
     * @param response returns 200 unless there's an error outside of channel creation
     * @throws IOException
     */
    @RequestMapping(value="/system/update/barb/channels",method=RequestMethod.POST)
    public void forceIngest(
            @RequestParam("channels") String channels,
            @Context HttpServletResponse response
    ) throws IOException {

        Map<String, String> createdChannels = Maps.newHashMap();
        List<String> updatedChannels = Lists.newArrayList();
        List<String> failedChannels = Lists.newArrayList();

        String[] splitChannels = channels.split("\\|");

        for (String channel: splitChannels) {
            try {
                Channel newChannel = barbChannelTransformer.transform(channel);
                channelWriter.createOrUpdate(newChannel);

                if (newChannel.getId() == null) {
                    updatedChannels.add(newChannel.getUri());
                } else {
                    createdChannels.put(
                            codec.encode(BigInteger.valueOf(newChannel.getId())),
                            newChannel.getTitle()
                    );
                }
            } catch (Exception e) {
                log.error("Error creating/updating channel {}", channel, e);
                failedChannels.add(channel);
            }
        }

        BarbChannelIngestResponse resp = BarbChannelIngestResponse.create(
                createdChannels,
                updatedChannels,
                failedChannels,
                getFinishMessage(createdChannels, updatedChannels, splitChannels)
        );

        response.getWriter().write(mapper.writeValueAsString(resp));
    }


//    @RequestMapping(value="/system/update/barb/equiv",method=RequestMethod.POST)
//    public void forceIngest(
//            @Context HttpServletResponse response
//    ) throws IOException {
//
//    }

    private String getFinishMessage(Map<String, String> created, List<String> updated, String[] total) {
        return format("Processed %d of %d", created.size() + updated.size(), total.length);
    }
}

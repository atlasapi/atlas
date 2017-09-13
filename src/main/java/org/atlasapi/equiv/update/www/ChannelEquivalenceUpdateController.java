package org.atlasapi.equiv.update.www;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.repackaged.com.google.common.base.Strings;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.gdata.util.common.base.Nullable;

import com.metabroadcast.columbus.telescope.api.Event;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.stream.MoreCollectors;
import org.atlasapi.equiv.update.EquivalenceUpdater;
import org.atlasapi.equiv.update.metadata.EquivalenceUpdaterMetadata;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.reporting.telescope.OwlTelescopeReporter;
import org.atlasapi.reporting.telescope.OwlTelescopeReporterFactory;
import org.atlasapi.reporting.telescope.OwlTelescopeReporters;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.math.BigInteger;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.StreamSupport;

@Controller
public class ChannelEquivalenceUpdateController {

    private static final Logger log = LoggerFactory.getLogger(ChannelEquivalenceUpdateController.class);

    private final Splitter commaSplitter = Splitter.on(',').trimResults().omitEmptyStrings();

    private final ChannelResolver channelResolver;
    private final EquivalenceUpdater<Channel> channelUpdater;
    private final SubstitutionTableNumberCodec codec;
    private final ExecutorService executor;
    private final ObjectMapper mapper;

    private ChannelEquivalenceUpdateController(
            EquivalenceUpdater<Channel> channelUpdater,
            ChannelResolver channelResolver
    ) {
        this.channelUpdater = channelUpdater;
        this.channelResolver = channelResolver;

        this.codec = SubstitutionTableNumberCodec.lowerCaseOnly();
        this.executor = Executors.newFixedThreadPool(5);
        this.mapper = new ObjectMapper();
    }

    public static ChannelEquivalenceUpdateController create(
            EquivalenceUpdater<Channel> channelUpdater,
            ChannelResolver channelResolver
    ) {
        return new ChannelEquivalenceUpdateController(channelUpdater, channelResolver);
    }

    @RequestMapping(value = "/system/equivalence/channel/update", method = RequestMethod.POST)
    public void runUpdate(
            HttpServletResponse response,
            @RequestParam(value = "uris", required = false, defaultValue = "") String uris,
            @RequestParam(value = "ids", required = false, defaultValue = "") String ids
    ) throws IOException {

        if (Strings.isNullOrEmpty(uris) && Strings.isNullOrEmpty(ids)) {
            throw new IllegalArgumentException("Must specify at least one of 'uris' or 'ids'");
        }

        ImmutableList<Long> longIds = StreamSupport.stream(commaSplitter.split(ids).spliterator(), false)
                .map(codec::decode)
                .map(BigInteger::longValue)
                .collect(MoreCollectors.toImmutableList());

        List<String> splitUris = Lists.newArrayList(commaSplitter.split(uris));

        ImmutableSet<Channel> channels = StreamSupport.stream(channelResolver.all().spliterator(), false)
                .filter(channel -> longIds.contains(channel.getId()) ||
                        splitUris.contains(channel.getUri()))
                .collect(MoreCollectors.toImmutableSet());

        if(channels.isEmpty()) {
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            return;
        }

        OwlTelescopeReporter telescope = OwlTelescopeReporterFactory.getInstance().getTelescopeReporter(
                OwlTelescopeReporters.MANUAL_CHANNEL_EQUIVALENCE,
                Event.Type.EQUIVALENCE
        );

        telescope.startReporting();

        channels.forEach(channel -> executor.submit(updateFor(channel, telescope)));

        telescope.endReporting();

        response.setStatus(HttpServletResponse.SC_OK);
    }

    private Runnable updateFor(Channel channel, OwlTelescopeReporter telescope) {
        return () -> {
            try {
                channelUpdater.updateEquivalences(channel, telescope);
                log.info("Finished updating {}", channel);
            } catch (Exception e) {
                log.error("Error updating equivalence for channel: {}", channel, e);
                telescope.reportFailedEvent(
                        e.toString(),
                        channel
                );
            }
        };
    }

    @RequestMapping(value = "/system/equivalence/channel/configuration", method = RequestMethod.GET)
    public void getEquivalenceConfiguration (
            HttpServletResponse response,
            @Nullable @RequestParam(value = "sources", required = false) List<String> sources
    ) throws IOException {

        ImmutableSet<Publisher> requestedSources;

        if (sources != null) {
            requestedSources = sources.stream()
                    .map(Publisher::fromKey)
                    .map(Maybe::requireValue)
                    .collect(MoreCollectors.toImmutableSet());
        } else {
            requestedSources  = Publisher.all();
        }

        EquivalenceUpdaterMetadata metadata = channelUpdater.getMetadata(requestedSources);

        mapper.writeValue(response.getWriter(), metadata);
        response.setStatus(HttpServletResponse.SC_OK);
    }

}

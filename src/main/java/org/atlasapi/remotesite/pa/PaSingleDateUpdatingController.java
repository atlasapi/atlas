package org.atlasapi.remotesite.pa;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import javax.annotation.PreDestroy;
import javax.servlet.http.HttpServletResponse;

import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.remotesite.pa.data.PaProgrammeDataStore;
import org.atlasapi.remotesite.pa.persistence.PaScheduleVersionStore;

import com.metabroadcast.common.base.Maybe;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import static com.google.common.base.Preconditions.checkNotNull;

@Controller
public class PaSingleDateUpdatingController  {
    
    private final ExecutorService executor;
    private final PaChannelProcessor channelProcessor;
    private final PaProgrammeDataStore fileManager;
	private final ChannelResolver channelResolver;
    private final PaScheduleVersionStore scheduleVersionStore;


    public PaSingleDateUpdatingController(
            PaChannelProcessor channelProcessor,
            ChannelResolver channelResolver,
            PaProgrammeDataStore fileManager,
            PaScheduleVersionStore paScheduleVersionStore
    ) {
        this.channelProcessor = channelProcessor;
        this.fileManager = fileManager;
        this.channelResolver = channelResolver;

        ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat("paSingleDateUpdater").build();
        this.executor = Executors.newSingleThreadExecutor(threadFactory);
        this.scheduleVersionStore = checkNotNull(paScheduleVersionStore);
    }
    
    @PreDestroy
    public void shutDown() {
        executor.shutdown();
    }

    @RequestMapping(
            value = "/system/update/pa/{dateString}",
            method = RequestMethod.POST,
            params = {}
    )
    public void runUpdate(
            @PathVariable String dateString,
            HttpServletResponse response
    ) {
        PaSingleDateUpdater updater = new PaSingleDateUpdater(
                Executors.newSingleThreadExecutor(),
                channelProcessor,
                fileManager,
                channelResolver,
                dateString,
                Optional.of(scheduleVersionStore)
        );
        executor.execute(updater);
        response.setStatus(HttpServletResponse.SC_OK);
    }
    
    @RequestMapping(
            value = "/system/update/pa/{dateString}",
            method = RequestMethod.POST,
            params = { "channel" }
    )
    public void runUpdate(
            @PathVariable String dateString,
            @RequestParam("channel") String channelUri,
            HttpServletResponse response
    ) {
        Maybe<Channel> channel = channelResolver.fromUri(channelUri);
        if (channel.hasValue()) {
            PaSingleDateUpdater updater = new PaSingleDateUpdater(
                    Executors.newSingleThreadExecutor(),
                    channelProcessor,
                    fileManager,
                    channelResolver,
                    dateString,
                    Optional.of(scheduleVersionStore)
            );
            updater.supportChannels(ImmutableList.of(channel.requireValue()));

            executor.execute(updater);
            response.setStatus(HttpServletResponse.SC_OK);
        } else {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        }
    }
}

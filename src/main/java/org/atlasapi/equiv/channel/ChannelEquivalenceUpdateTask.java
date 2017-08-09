package org.atlasapi.equiv.channel;

import com.metabroadcast.columbus.telescope.api.Event;
import com.metabroadcast.common.scheduling.ScheduledTask;
import org.atlasapi.equiv.update.EquivalenceUpdater;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelQuery;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.reporting.telescope.OwlTelescopeProxy;
import org.atlasapi.reporting.telescope.OwlTelescopeReporters;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class ChannelEquivalenceUpdateTask extends ScheduledTask {

    private static final Logger log = LoggerFactory.getLogger(ChannelEquivalenceUpdateTask.class);

    private final ChannelResolver channelResolver;
    private final Publisher publisher;
    private final EquivalenceUpdater<Channel> updater;

    private ChannelEquivalenceUpdateTask(
            ChannelResolver channelResolver,
            Publisher publisher,
            EquivalenceUpdater<Channel> updater
    ) {
        this.channelResolver = checkNotNull(channelResolver);
        this.publisher = checkNotNull(publisher);
        this.updater = checkNotNull(updater);
    }

    public static ChannelEquivalenceUpdateTask create(
            ChannelResolver channelResolver,
            Publisher publisher,
            EquivalenceUpdater<Channel> updater
    ) {
        return new ChannelEquivalenceUpdateTask(channelResolver, publisher, updater);
    }

    @Override
    public void runTask() {
        OwlTelescopeProxy telescopeProxy = OwlTelescopeProxy.create(
                OwlTelescopeReporters.EQUIVALENCE,
                Event.Type.EQUIVALENCE
        );

        telescopeProxy.startReporting();

        Iterable<Channel> publishersChannels = channelResolver.allChannels(
                ChannelQuery.builder().withPublisher(publisher).build()
        );

        log.info("Started channel equiv update for {}", publisher);
        publishersChannels.forEach(channel -> updater.updateEquivalences(channel, telescopeProxy));
        log.info("Finished channel equiv update for {}", publisher);

        telescopeProxy.endReporting();
    }

}

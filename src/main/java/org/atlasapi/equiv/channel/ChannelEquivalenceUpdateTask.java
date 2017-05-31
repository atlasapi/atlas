package org.atlasapi.equiv.channel;

import com.metabroadcast.common.scheduling.ScheduledTask;
import org.atlasapi.equiv.update.EquivalenceUpdater;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelQuery;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.entity.Publisher;

import static com.google.common.base.Preconditions.checkNotNull;

public class ChannelEquivalenceUpdateTask extends ScheduledTask {

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
        Iterable<Channel> publishersChannels = channelResolver.allChannels(
                ChannelQuery.builder().withPublisher(publisher).build()
        );

        publishersChannels.forEach(updater::updateEquivalences);

    }

}

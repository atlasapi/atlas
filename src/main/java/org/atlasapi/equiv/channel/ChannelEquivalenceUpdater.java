package org.atlasapi.equiv.channel;

import com.google.api.client.util.Maps;
import org.atlasapi.equiv.update.EquivalenceUpdater;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.entity.Publisher;

import java.util.Map;

public class ChannelEquivalenceUpdater implements EquivalenceUpdater<Channel> {

    private Map<Publisher, ChannelEquivalenceUpdater>

    private ChannelEquivalenceUpdater() {
        this.updaters = Maps.newHashMap();
    }

}

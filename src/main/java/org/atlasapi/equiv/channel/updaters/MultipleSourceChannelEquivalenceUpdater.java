package org.atlasapi.equiv.channel.updaters;

import com.google.api.client.util.Maps;
import com.metabroadcast.common.stream.MoreCollectors;
import org.atlasapi.equiv.update.EquivalenceUpdater;
import org.atlasapi.equiv.update.metadata.EquivalenceUpdaterMetadata;
import org.atlasapi.equiv.update.metadata.MultipleSourceEquivalenceUpdaterMetadata;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.entity.Publisher;

import java.util.Comparator;
import java.util.Map;
import java.util.Set;

public class MultipleSourceChannelEquivalenceUpdater implements EquivalenceUpdater<Channel> {

    private Map<Publisher, EquivalenceUpdater<Channel>> updaters;

    private MultipleSourceChannelEquivalenceUpdater() {
        this.updaters = Maps.newHashMap();
    }

    public static MultipleSourceChannelEquivalenceUpdater create() {
        return new MultipleSourceChannelEquivalenceUpdater();
    }

    public void register(Publisher publisher, EquivalenceUpdater<Channel> updater) {
        updaters.put(publisher, updater);
    }

    @Override
    public boolean updateEquivalences(Channel channel) {
        return updaters.get(channel.getSource()).updateEquivalences(channel);
    }

    @Override
    public EquivalenceUpdaterMetadata getMetadata(Set<Publisher> sources) {
        return MultipleSourceEquivalenceUpdaterMetadata.create(
                updaters.entrySet()
                        .stream()
                        .filter(entry -> sources.contains(entry.getKey()))
                        .sorted(Comparator.comparing(Map.Entry::getKey))
                        .collect(MoreCollectors.toImmutableMap(
                                entry -> entry.getKey().key(),
                                entry -> entry.getValue().getMetadata(sources)
                        ))
        );


    }
}

package org.atlasapi.equiv.update;

import java.util.Map;
import java.util.Optional;

import org.atlasapi.equiv.update.metadata.EquivalenceUpdaterMetadata;
import org.atlasapi.equiv.update.metadata.MultipleSourceEquivalenceUpdaterMetadata;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Publisher;

import com.metabroadcast.columbus.telescope.client.IngestTelescopeClientImpl;
import com.metabroadcast.common.stream.MoreCollectors;

import com.google.common.collect.Maps;

public class MultipleSourceEquivalenceUpdater implements EquivalenceUpdater<Content> {

    private Map<Publisher, EquivalenceUpdater<Content>> updaters;

    private MultipleSourceEquivalenceUpdater() {
        this.updaters = Maps.newHashMap();
    }

    public static MultipleSourceEquivalenceUpdater create() {
        return new MultipleSourceEquivalenceUpdater();
    }

    public MultipleSourceEquivalenceUpdater register(
            Publisher publisher,
            EquivalenceUpdater<Content> updater
    ) {
        updaters.put(publisher, updater);
        return this;
    }

    @Override
    public boolean updateEquivalences(
            Content subject,
            Optional<String> taskId,
            IngestTelescopeClientImpl telescopeClient
    ) {
        return updaters.get(subject.getPublisher())
                .updateEquivalences(subject, taskId, telescopeClient);
    }

    @Override
    public EquivalenceUpdaterMetadata getMetadata() {
        return MultipleSourceEquivalenceUpdaterMetadata.create(
                updaters.entrySet()
                        .stream()
                        .collect(MoreCollectors.toImmutableMap(
                                entry -> entry.getKey().key(),
                                entry -> entry.getValue().getMetadata()
                        ))
        );
    }
}

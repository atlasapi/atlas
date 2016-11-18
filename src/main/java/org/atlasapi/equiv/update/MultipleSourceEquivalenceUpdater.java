package org.atlasapi.equiv.update;

import java.util.Map;
import java.util.Set;

import org.atlasapi.equiv.update.metadata.EquivalenceUpdaterMetadata;
import org.atlasapi.equiv.update.metadata.MultipleSourceEquivalenceUpdaterMetadata;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Publisher;

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
    public boolean updateEquivalences(Content subject) {
        return updaters.get(subject.getPublisher()).updateEquivalences(subject);
    }

    @Override
    public EquivalenceUpdaterMetadata getMetadata(Set<Publisher> sources) {
        return MultipleSourceEquivalenceUpdaterMetadata.create(
                updaters.entrySet()
                        .stream()
                        .filter(entry -> sources.contains(entry.getKey()))
                        .sorted((o1, o2) -> o1.getKey().compareTo(o2.getKey()))
                        .collect(MoreCollectors.toImmutableMap(
                                entry -> entry.getKey().key(),
                                entry -> entry.getValue().getMetadata(sources)
                        ))
        );
    }
}

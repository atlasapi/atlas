package org.atlasapi.equiv.update;

import java.util.Comparator;
import java.util.Map;
import java.util.Set;

import org.atlasapi.equiv.update.metadata.EquivalenceUpdaterMetadata;
import org.atlasapi.equiv.update.metadata.MultipleSourceEquivalenceUpdaterMetadata;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.reporting.telescope.OwlTelescopeProxy;

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
    public boolean updateEquivalences(Content subject, OwlTelescopeProxy telescopeProxy) {
        return updaters.get(subject.getPublisher()).updateEquivalences(subject, telescopeProxy);
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

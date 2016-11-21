package org.atlasapi.equiv.generators.metadata;

import java.util.stream.StreamSupport;

import org.atlasapi.media.entity.Publisher;

import com.metabroadcast.common.stream.MoreCollectors;

import com.google.common.collect.ImmutableSet;

import static com.google.common.base.Preconditions.checkNotNull;

public class SourceLimitedEquivalenceGeneratorMetadata implements EquivalenceGeneratorMetadata {

    private final String name;
    private final ImmutableSet<String> sources;

    private SourceLimitedEquivalenceGeneratorMetadata(
            String name,
            ImmutableSet<String> sources
    ) {
        this.name = checkNotNull(name);
        this.sources = sources;
    }

    public static SourceLimitedEquivalenceGeneratorMetadata create(
            String name,
            Iterable<Publisher> sources
    ) {
        ImmutableSet<String> sourceKeys = StreamSupport
                .stream(sources.spliterator(), false)
                .map(Publisher::key)
                .collect(MoreCollectors.toImmutableSet());

        return new SourceLimitedEquivalenceGeneratorMetadata(name, sourceKeys);
    }

    public String getName() {
        return name;
    }

    public ImmutableSet<String> getSources() {
        return sources;
    }
}

package org.atlasapi.equiv.update.metadata;

import java.util.stream.StreamSupport;

import com.metabroadcast.common.stream.MoreCollectors;

import com.google.common.collect.ImmutableList;

public abstract class EquivalenceUpdaterMetadata {

    private final String name;

    protected EquivalenceUpdaterMetadata() {
        this.name = this.getClass().getSimpleName();
    }

    public String getName() {
        return name;
    }

    protected static String getClassName(Object object) {
        return object.getClass().getCanonicalName();
    }

    protected static ImmutableList<String> getClassName(Iterable<?> objects) {
        return StreamSupport.stream(objects.spliterator(), false)
                .map(EquivalenceUpdaterMetadata::getClassName)
                .collect(MoreCollectors.toImmutableList());
    }
}

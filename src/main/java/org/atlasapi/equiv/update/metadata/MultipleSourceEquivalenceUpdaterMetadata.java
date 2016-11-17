package org.atlasapi.equiv.update.metadata;

import java.util.Map;

import com.google.common.collect.ImmutableMap;

public class MultipleSourceEquivalenceUpdaterMetadata extends EquivalenceUpdaterMetadata {

    private final ImmutableMap<String, EquivalenceUpdaterMetadata> perSourceUpdaterMetadata;

    private MultipleSourceEquivalenceUpdaterMetadata(
            Map<String, EquivalenceUpdaterMetadata> perSourceUpdaterMetadata
    ) {
        this.perSourceUpdaterMetadata = ImmutableMap.copyOf(perSourceUpdaterMetadata);
    }

    public static MultipleSourceEquivalenceUpdaterMetadata create(
            Map<String, EquivalenceUpdaterMetadata> perSourceUpdaterMetadata
    ) {
        return new MultipleSourceEquivalenceUpdaterMetadata(perSourceUpdaterMetadata);
    }

    public ImmutableMap<String, EquivalenceUpdaterMetadata> getPerSourceUpdaterMetadata() {
        return perSourceUpdaterMetadata;
    }
}

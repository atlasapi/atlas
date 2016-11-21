package org.atlasapi.equiv.generators.metadata;

import static com.google.common.base.Preconditions.checkNotNull;

public class GenericEquivalenceGeneratorMetadata implements EquivalenceGeneratorMetadata {

    private final String name;

    private GenericEquivalenceGeneratorMetadata(String name) {
        this.name = checkNotNull(name);
    }

    public static GenericEquivalenceGeneratorMetadata create(String name) {
        return new GenericEquivalenceGeneratorMetadata(name);
    }

    public String getName() {
        return name;
    }
}

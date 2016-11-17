package org.atlasapi.equiv.update.metadata;

public class NopEquivalenceUpdaterMetadata extends EquivalenceUpdaterMetadata {

    private NopEquivalenceUpdaterMetadata() {
    }

    public static NopEquivalenceUpdaterMetadata create() {
        return new NopEquivalenceUpdaterMetadata();
    }
}

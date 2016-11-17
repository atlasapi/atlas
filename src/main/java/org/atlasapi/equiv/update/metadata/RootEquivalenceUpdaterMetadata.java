package org.atlasapi.equiv.update.metadata;

import static com.google.common.base.Preconditions.checkNotNull;

public class RootEquivalenceUpdaterMetadata extends EquivalenceUpdaterMetadata {

    private final EquivalenceUpdaterMetadata delegateMetadata;

    private RootEquivalenceUpdaterMetadata(EquivalenceUpdaterMetadata delegateMetadata) {
        this.delegateMetadata = checkNotNull(delegateMetadata);
    }

    public EquivalenceUpdaterMetadata getDelegateMetadata() {
        return delegateMetadata;
    }

    public static RootEquivalenceUpdaterMetadata create(
            EquivalenceUpdaterMetadata delegateMetadata
    ) {
        return new RootEquivalenceUpdaterMetadata(delegateMetadata);
    }
}

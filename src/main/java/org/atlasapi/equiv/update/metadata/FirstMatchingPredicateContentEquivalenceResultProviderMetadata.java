package org.atlasapi.equiv.update.metadata;

import com.google.common.collect.ImmutableList;

import static com.google.common.base.Preconditions.checkNotNull;

public class FirstMatchingPredicateContentEquivalenceResultProviderMetadata extends EquivalenceUpdaterMetadata {

    private final ImmutableList<EquivalenceUpdaterMetadata> equivalenceUpdaterMetadata;
    private final String predicate;

    private FirstMatchingPredicateContentEquivalenceResultProviderMetadata(
            Iterable<EquivalenceUpdaterMetadata> equivalenceUpdaterMetadata,
            String predicate
    ) {
        this.equivalenceUpdaterMetadata = ImmutableList.copyOf(equivalenceUpdaterMetadata);
        this.predicate = checkNotNull(predicate);
    }

    public static FirstMatchingPredicateContentEquivalenceResultProviderMetadata create(
            Iterable<EquivalenceUpdaterMetadata> equivalenceUpdaterMetadata,
            String predicate
    ) {
        return new FirstMatchingPredicateContentEquivalenceResultProviderMetadata(
                equivalenceUpdaterMetadata,
                predicate
        );
    }

    public ImmutableList<EquivalenceUpdaterMetadata> getEquivalenceUpdaterMetadata() {
        return equivalenceUpdaterMetadata;
    }

    public String getPredicate() {
        return predicate;
    }
}

package org.atlasapi.equiv.update;

import com.google.common.collect.ImmutableList;
import com.metabroadcast.common.stream.MoreCollectors;
import org.atlasapi.equiv.results.EquivalenceResult;
import org.atlasapi.equiv.results.description.ReadableDescription;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResults;
import org.atlasapi.equiv.update.metadata.EquivalenceUpdaterMetadata;
import org.atlasapi.equiv.update.metadata.FirstMatchingPredicateContentEquivalenceResultProviderMetadata;
import org.atlasapi.media.entity.Content;

import java.util.List;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * This EquivalenceResultUpdater takes a list of EquivalenceResultUpdater delegates and calls provideEquivalenceResult
 * on each in order; the result returned is the first result which matches the given predicate
 * The last equivalence result is returned if none match the given predicate but this is behaviour that should not be
 * relied upon; it exists solely to return something meaningful.
 */
public class FirstMatchingPredicateContentEquivalenceResultUpdater<T extends Content> implements EquivalenceResultUpdater<T> {

    private final List<EquivalenceResultUpdater<T>> equivalenceResultUpdaters;
    private final EquivalencePredicate<T> equivalenceResultPredicate;

    private final EquivalenceUpdaterMetadata metadata;

    private FirstMatchingPredicateContentEquivalenceResultUpdater(
            Iterable<EquivalenceResultUpdater<T>> equivalenceResultUpdaters,
            EquivalencePredicate<T> equivalenceResultPredicate
    ) {
        this.equivalenceResultUpdaters = ImmutableList.copyOf(equivalenceResultUpdaters);
        checkArgument(!this.equivalenceResultUpdaters.isEmpty());
        this.equivalenceResultPredicate = checkNotNull(equivalenceResultPredicate);

        this.metadata = FirstMatchingPredicateContentEquivalenceResultProviderMetadata.create(
                this.equivalenceResultUpdaters.stream()
                        .map(EquivalenceResultUpdater::getMetadata)
                        .collect(MoreCollectors.toImmutableList()),
                equivalenceResultPredicate.getPredicateName()
        );
    }

    public static <T extends Content> FirstMatchingPredicateContentEquivalenceResultUpdater<T> create(
            Iterable<EquivalenceResultUpdater<T>> equivalenceResultUpdaters,
            EquivalencePredicate<T> equivalenceResultPredicate
    ) {
        return new FirstMatchingPredicateContentEquivalenceResultUpdater<>(
                equivalenceResultUpdaters,
                equivalenceResultPredicate
        );
    }

    public static <T extends Content> FirstMatchingPredicateContentEquivalenceResultUpdater<T> create(
            Iterable<EquivalenceResultUpdater<T>> equivalenceResultUpdaters,
            Predicate<EquivalenceResult<T>> equivalenceResultPredicate,
            String equivalenceResultPredicateName
    ) {
        return new FirstMatchingPredicateContentEquivalenceResultUpdater<>(
                equivalenceResultUpdaters,
                new EquivalencePredicate<>(equivalenceResultPredicate, equivalenceResultPredicateName)
        );
    }

    @Override
    public EquivalenceResult<T> provideEquivalenceResult(
            T content,
            ReadableDescription desc,
            EquivToTelescopeResults resultsForTelescope
    ) {
        checkArgument(!equivalenceResultUpdaters.isEmpty());
        EquivalenceResult<T> result = null;
        desc.startStage("First Matching Predicate: " + equivalenceResultPredicate.getPredicateName());
        int count = 1;
        for (EquivalenceResultUpdater<T> equivalenceResultUpdater : equivalenceResultUpdaters) {
            desc.startStage("EquivalenceResultUpdater " + count++);
            result = equivalenceResultUpdater.provideEquivalenceResult(
                    content,
                    desc,
                    resultsForTelescope
            );
            desc.finishStage();
            if (equivalenceResultPredicate.getPredicate().test(result)) {
                desc.finishStage();
                return result;
            }
        }
        desc.finishStage();

        return result; //will return the result of the last updater if none matched the predicate
    }

    @Override
    public EquivalenceUpdaterMetadata getMetadata() {
        return metadata;
    }

    public static class EquivalencePredicate<T extends Content> {
        private final Predicate<EquivalenceResult<T>> predicate;
        private final String predicateName;

        public EquivalencePredicate(Predicate<EquivalenceResult<T>> predicate, String predicateName) {
            this.predicate = checkNotNull(predicate);
            this.predicateName = checkNotNull(predicateName);
        }

        public Predicate<EquivalenceResult<T>> getPredicate() {
            return predicate;
        }

        public String getPredicateName() {
            return predicateName;
        }
    }


}

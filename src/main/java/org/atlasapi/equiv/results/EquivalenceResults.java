package org.atlasapi.equiv.results;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.metabroadcast.common.stream.MoreCollectors;
import org.atlasapi.equiv.results.description.ReadableDescription;
import org.atlasapi.equiv.results.scores.ScoredCandidate;

import java.util.Collection;
import java.util.List;
import java.util.Set;

public class EquivalenceResults<T> {

    private final T subject;
    private final List<EquivalenceResult<T>> results;
    private final ReadableDescription desc;

    public EquivalenceResults(T subject, Iterable<EquivalenceResult<T>> results, ReadableDescription desc) {
        this.subject = subject;
        this.results = ImmutableList.copyOf(results);
        this.desc = desc;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(getClass())
            .addValue(subject)
            .add("results", results)
            .toString();
    }
    
    @Override
    public boolean equals(Object that) {
        if(this == that) {
            return true;
        }
        if(that instanceof EquivalenceResults) {
            EquivalenceResults<?> other = (EquivalenceResults<?>) that;
            return Objects.equal(subject, other.subject) && Objects.equal(results, other.results);
        }
        return false;
    }
    
    @Override
    public int hashCode() {
        return Objects.hashCode(subject, results);
    }

    public T subject() {
        return subject;
    }

    public List<EquivalenceResult<T>> getResults() {
        return results;
    }

    public ReadableDescription description() {
        return desc;
    }

    public Set<T> strongEquivalences() {
        return results.stream()
                .map(EquivalenceResult::strongEquivalences)
                .map(Multimap::values)
                .flatMap(Collection::stream)
                .map(ScoredCandidate::candidate)
                .collect(MoreCollectors.toImmutableSet());
    }
}

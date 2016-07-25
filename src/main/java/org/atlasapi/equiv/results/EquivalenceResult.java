package org.atlasapi.equiv.results;

import java.util.List;
import java.util.Map;

import org.atlasapi.equiv.results.description.ReadableDescription;
import org.atlasapi.equiv.results.scores.ScoredCandidate;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Publisher;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;

public class EquivalenceResult<T> {
    
    public static final <T extends Content> Function<EquivalenceResult<T>, T> toSubject() {
        return new Function<EquivalenceResult<T>, T>() {

            @Override
            public T apply(EquivalenceResult<T> input) {
                return input.subject();
            }
        };
    }

    private final T subject;
    private final List<ScoredCandidates<T>> scores;
    private final ScoredCandidates<T> combined;
    private final Multimap<Publisher, ScoredCandidate<T>> strong;
    private final ReadableDescription desc;

    public EquivalenceResult(T subject, List<ScoredCandidates<T>> scores, ScoredCandidates<T> combined, Multimap<Publisher, ScoredCandidate<T>> strong, ReadableDescription desc) {
        this.subject = subject;
        this.scores = ImmutableList.copyOf(scores);
        this.combined = combined;
        this.strong = ImmutableMultimap.copyOf(strong);
        this.desc = desc;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(getClass())
            .addValue(subject)
            .add("scores", scores)
            .add("combined", combined)
            .add("strong", strong)
            .toString();
    }
    
    @Override
    public boolean equals(Object that) {
        if(this == that) {
            return true;
        }
        if(that instanceof EquivalenceResult) {
            EquivalenceResult<?> other = (EquivalenceResult<?>) that;
            return Objects.equal(subject, other.subject) && Objects.equal(scores, other.scores);
        }
        return false;
    }
    
    @Override
    public int hashCode() {
        return Objects.hashCode(subject, scores);
    }
    
    public ScoredCandidates<T> combinedEquivalences() {
        return this.combined;
    }
    
    public Multimap<Publisher, ScoredCandidate<T>> strongEquivalences() {
        return strong;
    }

    public T subject() {
        return subject;
    }

    public List<ScoredCandidates<T>> rawScores() {
        return scores;
    }

    public ReadableDescription description() {
        return desc;
    }
    
}

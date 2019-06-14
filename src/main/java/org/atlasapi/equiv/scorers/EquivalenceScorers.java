package org.atlasapi.equiv.scorers;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResult;

import java.util.List;
import java.util.Set;

public class EquivalenceScorers<T> {
    
    public static <T> EquivalenceScorers<T> from(Iterable<? extends EquivalenceScorer<T>> generators) {
        return new EquivalenceScorers<T>(generators);
    }

    private final List<? extends EquivalenceScorer<T>> scorers;

    public EquivalenceScorers(Iterable<? extends EquivalenceScorer<T>> scorers) {
        this.scorers = ImmutableList.copyOf(scorers);
    }

    public List<ScoredCandidates<T>> score(
            T content,
            Set<? extends T> candidates,
            ResultDescription desc,
            EquivToTelescopeResult equivToTelescopeResult
    ) {
        desc.startStage("Scoring equivalences");
        Builder<ScoredCandidates<T>> scoredScores = ImmutableList.builder();

        for (EquivalenceScorer<T> scorer : scorers) {
            try {
                desc.startStage(scorer.toString());
                scoredScores.add(scorer.score(content, candidates, desc, equivToTelescopeResult));
                desc.finishStage();
            } catch (Exception e) {
                throw new RuntimeException(String.format("%s - %s", scorer, content), e);
            }
        }

        desc.finishStage();
        return scoredScores.build();
    }
    
    @Override
    public String toString() {
        return Objects.toStringHelper(getClass())
                .add("scorers", scorers)
                .toString();
    }
}

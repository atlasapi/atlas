package org.atlasapi.equiv.scorers;

import java.util.Set;

import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeComponent;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResults;
import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Item;

import static com.google.common.base.Preconditions.checkNotNull;

public class RtAliasScorer implements EquivalenceScorer<Item> {

    private static final String NAME = "Content-Alias-Scorer";
    private static final String NAMESPACE_TO_MATCH = "rt:filmid";

    private final Score mismatchScore;

    public RtAliasScorer(Score mismatchScore) {
        this.mismatchScore = checkNotNull(mismatchScore);
    }

    @Override
    public ScoredCandidates<Item> score(
            Item subject,
            Set<? extends Item> candidates,
            ResultDescription desc,
            EquivToTelescopeResults equivToTelescopeResults
    ) {
        DefaultScoredCandidates.Builder<Item> equivalents =
                DefaultScoredCandidates.fromSource(NAME);

        EquivToTelescopeComponent scorerComponent = EquivToTelescopeComponent.create();
        scorerComponent.setComponentName("Content Alias Scorer");

        candidates.forEach(candidate -> {
            Score score = score(subject, candidate);
            equivalents.addEquivalent(candidate, score);

            if (candidate.getId() != null) {
                scorerComponent.addComponentResult(
                        candidate.getId(),
                        String.valueOf(score.asDouble())
                );
            }
        });

        equivToTelescopeResults.addScorerResult(scorerComponent);

        return equivalents.build();
    }

    private Score score(Item subject, Item candidate) {

        //if aliases from same namespaces match, and namespace is the one we care about, then score one
        Set<Alias> aliasesOfCandidate = candidate.getAliases();
        Set<Alias> aliasesOfSubject = subject.getAliases();

        for (Alias alias : aliasesOfSubject) {
            if (isNamespaceTheDesiredOne(alias) && aliasesOfCandidate.contains(alias)) {
                return Score.ONE;
            }
        }

        return mismatchScore;
    }

    private boolean isNamespaceTheDesiredOne(Alias alias) {
        return (alias.getNamespace().equals(NAMESPACE_TO_MATCH));
    }

    @Override
    public String toString() {
        return NAME;
    }

}

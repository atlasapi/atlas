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

    private static final String NAME = "Rt-Alias-Scorer";
    private static final String NAMESPACE_TO_MATCH = "rt:filmid";
    private static final String NEW_PA_URL_FORMAT = "http://pressassociation.com/episodes/";

    private final Score mismatchScore;
    private final Score perfectMatchScore;

    public RtAliasScorer(Score mismatchScore, Score perfectMatchScore) {
        this.mismatchScore = checkNotNull(mismatchScore);
        this.perfectMatchScore = checkNotNull(perfectMatchScore);
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
        scorerComponent.setComponentName("Rt Alias Scorer");

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
            if (isNamespaceTheDesiredOne(alias)
                    && aliasesOfCandidate.contains(alias)
                    //score higher if candidate has new URL (to phase out old ones when both exist)
                    && candidate.getCanonicalUri().contains(NEW_PA_URL_FORMAT)) {
                return perfectMatchScore;
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

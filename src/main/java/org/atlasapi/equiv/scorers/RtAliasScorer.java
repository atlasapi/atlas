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

/**
 * This alias based scorer makes RT items equiv to PA items that are under the new URI format.
 * See the documentation regarding the RT equiv rework that this is part of:
 * https://docs.metabroadcast.com/display/mbst/(WIP)+RT+Equiv+Rework
 */

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
            Score score = score(subject, candidate, desc);
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

    private Score score(Item subject, Item candidate, ResultDescription desc) {

        Set<Alias> aliasesOfCandidate = candidate.getAliases();
        Set<Alias> aliasesOfSubject = subject.getAliases();

        for (Alias alias : aliasesOfSubject) {
            //check that the namespace is the one we care about
            if (alias.getNamespace().equals(NAMESPACE_TO_MATCH)
                    && aliasesOfCandidate.contains(alias)
                    //score high if candidate has new URL (to phase out old ones when both exist)
                    && candidate.getCanonicalUri().contains(NEW_PA_URL_FORMAT)) {
                desc.appendText(
                        "%s (%s) scored %s on alias with namespace %s and value %s",
                        candidate.getTitle(),
                        candidate.getCanonicalUri(),
                        perfectMatchScore,
                        alias.getNamespace(),
                        alias.getValue()
                );
                return perfectMatchScore;
            }
        }
        desc.appendText("%s (%s) ignored: no matching alias for namespace %s and/or URI not %s",
                candidate.getTitle(),
                candidate.getCanonicalUri(),
                NAMESPACE_TO_MATCH,
                NEW_PA_URL_FORMAT
        );
        return mismatchScore;
    }

    @Override
    public String toString() {
        return NAME;
    }

}

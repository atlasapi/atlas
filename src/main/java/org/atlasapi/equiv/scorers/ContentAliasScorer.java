package org.atlasapi.equiv.scorers;

import com.google.common.collect.Sets;
import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeComponent;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResult;
import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Item;

import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

public class ContentAliasScorer implements EquivalenceScorer<Item> {

    private static final String NAME = "Content-Alias-Scorer";

    private final Score mismatchScore;

    public ContentAliasScorer(Score mismatchScore) {
        this.mismatchScore = checkNotNull(mismatchScore);
    }

    @Override
    public ScoredCandidates<Item> score(
            Item subject,
            Set<? extends Item> candidates,
            ResultDescription desc,
            EquivToTelescopeResult equivToTelescopeResult
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

        equivToTelescopeResult.addScorerResult(scorerComponent);

        return equivalents.build();
    }

    private Score score(Item subject, Item candidate) {
        Set<String> aliasStringsOfCandidate = candidate.getAliasUrls();
        Set<String> aliasStringsOfSubject = subject.getAliasUrls();

        if (!Sets.intersection(aliasStringsOfCandidate, aliasStringsOfSubject).isEmpty()) {
            return Score.ONE;
        }

        Set<Alias> aliasesOfCandidate = candidate.getAliases();
        Set<Alias> aliasesOfSubject = subject.getAliases();

        if (!Sets.intersection(aliasesOfCandidate, aliasesOfSubject).isEmpty()) {
            return Score.ONE;
        }

        return mismatchScore;
    }

    @Override
    public String toString(){
        return NAME;
    }
}


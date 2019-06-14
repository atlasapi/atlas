package org.atlasapi.equiv.scorers;

import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeComponent;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResult;
import org.atlasapi.media.entity.Item;

import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

public class ItemYearScorer implements EquivalenceScorer<Item> {

    private static final String NAME = "Item-Year";

    private final Score matchScore;

    public ItemYearScorer(Score matchScore) {
        this.matchScore = checkNotNull(matchScore);
    }

    @Override
    public ScoredCandidates<Item> score(
            org.atlasapi.media.entity.Item subject,
            Set<? extends org.atlasapi.media.entity.Item> candidates,
            ResultDescription desc,
            EquivToTelescopeResult equivToTelescopeResult
    ) {
        EquivToTelescopeComponent scorerComponent = EquivToTelescopeComponent.create();
        scorerComponent.setComponentName("Item Year Scorer");
        DefaultScoredCandidates.Builder<Item> scoredCandidates = DefaultScoredCandidates.fromSource(NAME);

        for (Item candidate : candidates) {
            Score score = score(subject, candidate);

            scoredCandidates.addEquivalent(candidate, score);
            scorerComponent.addComponentResult(
                    candidate.getId(),
                    String.valueOf(score.asDouble())
            );
        }

        equivToTelescopeResult.addScorerResult(scorerComponent);

        return scoredCandidates.build();
    }

    private Score score(Item subject, Item candidate) {
        if (subject.getYear() == null || candidate.getYear() == null) {
            return Score.nullScore();
        }
        return subject.getYear().equals(candidate.getYear()) ? matchScore : Score.ZERO;
    }

    @Override
    public String toString(){
        return NAME;
    }
}

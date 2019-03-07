package org.atlasapi.equiv.scorers;

import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeComponent;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResults;
import org.atlasapi.media.entity.Item;

import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

public class ItemYearScorer implements EquivalenceScorer<Item> {

    private static final String NAME = "Item-Year";

    private static final Score defaultMismatchScore = Score.ZERO;
    private static final Score defaultNullYearScore = Score.nullScore();

    protected final Score matchScore;
    protected final Score mismatchScore;
    protected final Score nullYearScore;

    public ItemYearScorer(Score matchScore) {
        this.matchScore = checkNotNull(matchScore);
        this.mismatchScore = defaultMismatchScore;
        this.nullYearScore = defaultNullYearScore;
    }

    public ItemYearScorer(Score matchScore, Score mismatchScore, Score nullYearScore) {
        this.matchScore = checkNotNull(matchScore);
        this.mismatchScore = checkNotNull(mismatchScore);
        this.nullYearScore = checkNotNull(nullYearScore);
    }

    @Override
    public ScoredCandidates<Item> score(
            org.atlasapi.media.entity.Item subject,
            Set<? extends org.atlasapi.media.entity.Item> candidates,
            ResultDescription desc,
            EquivToTelescopeResults equivToTelescopeResults
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

        equivToTelescopeResults.addScorerResult(scorerComponent);

        return scoredCandidates.build();
    }

    protected Score score(Item subject, Item candidate) {
        if (subject.getYear() == null || candidate.getYear() == null) {
            return nullYearScore;
        }
        return subject.getYear().equals(candidate.getYear()) ? matchScore : mismatchScore;
    }

    @Override
    public String toString(){
        return NAME;
    }
}

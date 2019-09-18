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

/**
 * This will score a candidate if and only if it has the exact same Year as the subject.
 *
 * Default scores are zero on mismatch, and null if either are missing a Year.
 */
public class ItemYearScorer implements EquivalenceScorer<Item> {

    private static final String NAME = "Item-Year";

    private static final Score DEFAULT_MISMATCH_SCORE = Score.ZERO;
    private static final Score DEFAULT_NULL_YEAR_SCORE = Score.nullScore();

    protected final Score matchScore;
    protected final Score mismatchScore;
    protected final Score nullYearScore;

    public ItemYearScorer(Score matchScore) {
        this.matchScore = checkNotNull(matchScore);
        this.mismatchScore = DEFAULT_MISMATCH_SCORE;
        this.nullYearScore = DEFAULT_NULL_YEAR_SCORE;
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

package org.atlasapi.equiv.scorers;

import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeComponent;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResult;
import org.atlasapi.media.entity.Film;
import org.atlasapi.media.entity.Item;

import java.util.Set;

/**
 * Like {@link ItemYearScorer}, except
 * only scores candidate if and only if both it and the subject are a film.
 */
public class FilmYearScorer extends ItemYearScorer {
    private static final String NAME = "Film-Year";

    public FilmYearScorer(Score matchScore) {
        super(matchScore);
    }

    public FilmYearScorer(Score matchScore, Score mismatchScore, Score nullYearScore) {
        super(matchScore, mismatchScore, nullYearScore);
    }

    @Override
    public ScoredCandidates<Item> score(
            Item subject,
            Set<? extends Item> candidates,
            ResultDescription desc,
            EquivToTelescopeResult equivToTelescopeResults
    ) {
        EquivToTelescopeComponent scorerComponent = EquivToTelescopeComponent.create();
        scorerComponent.setComponentName("Film Year Scorer");
        DefaultScoredCandidates.Builder<Item> scoredCandidates = DefaultScoredCandidates.fromSource(NAME);

        for (Item candidate : candidates) {
            Score score = (subject instanceof Film && candidate instanceof Film)
                    ? score(subject, candidate)
                    : Score.nullScore();

            scoredCandidates.addEquivalent(candidate, score);
            scorerComponent.addComponentResult(
                    candidate.getId(),
                    String.valueOf(score.asDouble())
            );
        }

        equivToTelescopeResults.addScorerResult(scorerComponent);

        return scoredCandidates.build();
    }

    @Override
    public String toString() {
        return NAME;
    }
}

package org.atlasapi.equiv.scorers;

import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeComponent;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResults;
import org.atlasapi.media.entity.Container;

import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

public class ContainerYearScorer implements EquivalenceScorer<Container> {

    private static final String YEAR = "Container-Year";

    private final Score matchScore;

    public ContainerYearScorer(Score matchScore) {
        this.matchScore = checkNotNull(matchScore);
    }

    @Override
    public ScoredCandidates<Container> score(
            Container subject,
            Set<? extends Container> candidates,
            ResultDescription desc,
            EquivToTelescopeResults equivToTelescopeResults
    ) {
        EquivToTelescopeComponent scorerComponent = EquivToTelescopeComponent.create();
        scorerComponent.setComponentName("Container Year Scorer");
        DefaultScoredCandidates.Builder<Container> scoredCandidates = DefaultScoredCandidates.fromSource(YEAR);

        for (Container candidate : candidates) {
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

    private Score score(Container subject, Container candidate) {
        if (subject.getYear() == null || candidate.getYear() == null) {
            return Score.nullScore();
        }
        return subject.getYear().equals(candidate.getYear()) ? matchScore : Score.ZERO;
    }
}

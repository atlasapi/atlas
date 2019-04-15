package org.atlasapi.equiv.scorers;

import com.google.common.collect.Iterables;
import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates.Builder;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeComponent;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResult;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Series;

import java.util.Set;


public class SequenceContainerScorer implements EquivalenceScorer<Container> {

    @Override
    public ScoredCandidates<Container> score(
            Container subject,
            Set<? extends Container> candidates,
            ResultDescription desc,
            EquivToTelescopeResult equivToTelescopeResult
    ) {
        EquivToTelescopeComponent scorerComponent = EquivToTelescopeComponent.create();
        scorerComponent.setComponentName("Sequence Container Scorer");

        Builder<Container> scores = DefaultScoredCandidates.fromSource("Sequence");
        
        if (!(subject instanceof Series)) {
            desc.appendText("subject %s not Series", subject.getClass());
            return scoreAllNull(scores, candidates, scorerComponent);
        }
        
        Series series = (Series) subject;
        if (series.getParent() == null) {
            desc.appendText("subject is top level");
            return scoreAllNull(scores, candidates, scorerComponent);
        }

        if (series.getSeriesNumber() == null) {
            desc.appendText("subject has no series number");
            return scoreAllNull(scores, candidates, scorerComponent);
        }
        desc.appendText("subject series number: ", series.getSeriesNumber());
                    
        for (Series candidate : Iterables.filter(candidates, Series.class)) {
            Score score;
            if (candidate.getParent() == null) {
                score = Score.ZERO;
                desc.appendText("%s: top-level: %s", candidate, score);
            } else if (candidate.getSeriesNumber() == null) {
                score = Score.nullScore();
                desc.appendText("%s: no series number: %s", candidate, score);
            } else if (series.getSeriesNumber().equals(candidate.getSeriesNumber())) {
                score = Score.ONE;
                desc.appendText("%s: series number: %s: %s", candidate, candidate.getSeriesNumber(), score);
            } else {
                score = Score.ZERO;
                desc.appendText("%s: series number: %s: %s", candidate, candidate.getSeriesNumber(), score);
            }
            scores.addEquivalent(candidate, score);
            scorerComponent.addComponentResult(
                    candidate.getId(),
                    String.valueOf(score.asDouble())
            );
        }

        return scores.build();
    }

    private ScoredCandidates<Container> scoreAllNull(
            Builder<Container> scores,
            Set<? extends Container> candidates,
            EquivToTelescopeComponent scorerComponent
    ) {
        for (Container container : candidates) {
            scores.addEquivalent(container, Score.nullScore());
            scorerComponent.addComponentResult(
                    container.getId(),
                    ""
            );
        }
        return scores.build();
    }

    @Override
    public String toString() {
        return "Container sequence scorer";
    }
    
}

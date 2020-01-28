package org.atlasapi.equiv.scorers;

import java.util.Set;

import org.atlasapi.equiv.generators.ContentTitleScorer;
import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeComponent;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResult;
import org.atlasapi.media.entity.Container;

import com.google.common.base.Functions;

public class TitleMatchingContainerScorer implements EquivalenceScorer<Container> {

    public static final String NAME = "Title";
    
    private final ContentTitleScorer<Container> scorer;

    public TitleMatchingContainerScorer(double exactTitleMatchScore) {
        this(Score.valueOf(exactTitleMatchScore), Score.ONE, true);
    }

    public TitleMatchingContainerScorer(
            Score exactTitleMatchScore,
            Score partialTitleMatchBound,
            boolean scaleOnPartialMatch
    ) {
        this.scorer = new ContentTitleScorer<>(
                NAME,
                Functions.<String>identity(),
                exactTitleMatchScore,
                partialTitleMatchBound,
                scaleOnPartialMatch
        );
    }
    
    @Override
    public ScoredCandidates<Container> score(
            Container subject,
            Set<? extends Container> candidates,
            ResultDescription desc,
            EquivToTelescopeResult equivToTelescopeResult
    ) {
        EquivToTelescopeComponent scorerComponent = EquivToTelescopeComponent.create();
        scorerComponent.setComponentName("Title Matching Container Scorer");

        return scorer.scoreCandidates(subject, candidates, desc, scorerComponent);
    }

    @Override
    public String toString() {
        return "Title-matching Scorer";
    }
}

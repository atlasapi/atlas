package org.atlasapi.equiv.scorers;

import com.google.common.base.Functions;
import org.atlasapi.equiv.generators.ContentTitleScorer;
import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeComponent;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResult;
import org.atlasapi.media.entity.Container;

import java.util.Set;

public class TitleMatchingContainerScorer implements EquivalenceScorer<Container> {

    public static final String NAME = "Title";
    
    private final ContentTitleScorer<Container> scorer;

    public TitleMatchingContainerScorer(double exactTitleMatchScore) {
        this.scorer = new ContentTitleScorer<Container>(NAME, Functions.<String>identity(), exactTitleMatchScore);
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

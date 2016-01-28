package org.atlasapi.equiv.scorers;

import java.util.Set;

import org.atlasapi.equiv.generators.ContentTitleScorer;
import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.media.entity.Container;

import com.google.common.base.Functions;

public class TitleMatchingContainerScorer implements EquivalenceScorer<Container> {

    public static final String NAME = "Title";
    
    private final ContentTitleScorer<Container> scorer;

    public TitleMatchingContainerScorer(double exactTitleMatchScore) {
        this.scorer = new ContentTitleScorer<Container>(NAME, Functions.<String>identity(), exactTitleMatchScore);
    }
    
    @Override
    public ScoredCandidates<Container> score(Container subject, Set<? extends Container> candidates, ResultDescription desc) {
        return scorer.scoreCandidates(subject, candidates, desc);
    }

    @Override
    public String toString() {
        return "Title-matching Scorer";
    }
}

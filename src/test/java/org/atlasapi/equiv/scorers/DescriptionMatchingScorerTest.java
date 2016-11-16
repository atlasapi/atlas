package org.atlasapi.equiv.scorers;

import org.atlasapi.equiv.results.description.DefaultDescription;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.media.entity.Item;

import com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DescriptionMatchingScorerTest {

    private DescriptionMatchingScorer scorer;

    @Before
    public void setUp() {
        this.scorer = DescriptionMatchingScorer.makeScorer();
    }

    private Score score(Item subject, Item candidate) {
        DefaultDescription desc = new DefaultDescription();
        ScoredCandidates<Item> scores = scorer.score(subject, ImmutableSet.of(candidate), desc);
        return scores.candidates().get(candidate);
    }

    @Test
    public void descriptionsMatchWhenSimilarEnough() {
        Item subject = new Item();
        Item candidate = new Item();
        subject.setDescription("Football related content and Manchester United talk");
        candidate.setDescription("Manchester united play Football");
        assertEquals(Score.ONE, score(candidate, subject));
        assertEquals(Score.ONE, score(subject, candidate));
    }

    @Test
    public void descriptionsMismatchWhenNotSimilar() {
        Item subject = new Item();
        Item candidate = new Item();
        subject.setDescription("Football related content and Manchester United talk");
        candidate.setDescription("Something nothing Actually related");
        assertEquals(Score.nullScore(), score(candidate, subject));
        assertEquals(Score.nullScore(), score(subject, candidate));
    }

    @Test
    public void capitalsAreNecessaryForMatching() {
        Item subject = new Item();
        Item candidate = new Item();
        subject.setDescription("Football related content and Manchester United talk");
        candidate.setDescription("football related content and manchester united talk");
        assertEquals(Score.nullScore(), score(candidate, subject));
        assertEquals(Score.nullScore(), score(subject, candidate));
    }

    @Test
    public void matchingDoesNotOccurJustBelowSpecifiedRange() {
        Item subjectToJustMatch = new Item();
        subjectToJustMatch.setDescription(
                "Five Capitalised Words Match Out of Ten "
                        + "Specific capitalised Words in Subject Provided"
        );
        Item candidateToJustMatch = new Item();
        candidateToJustMatch.setDescription(
                "Five Capitalised Words Match Out of ten "
                        + "specific capitalised words in subject provided"
        );

        assertEquals(Score.ONE, score(subjectToJustMatch, candidateToJustMatch));
        assertEquals(Score.ONE, score(candidateToJustMatch, subjectToJustMatch));
    }

    @Test
    public void matchingOccursJustAboveSpecificThreshold() {
        Item subjectToJustMismatch = new Item();
        subjectToJustMismatch.setDescription(
                "Four Capitalised Words Match Out of Ten "
                        + "Specific Chosen words in Subject Provided"
        );
        Item candidateToJustMismatch = new Item();
        candidateToJustMismatch.setDescription(
                "Four Capitalised Words Match out of ten "
                        + "specific capitalised words in subject provided"
        );

        assertEquals(Score.nullScore(), score(subjectToJustMismatch, candidateToJustMismatch));
        assertEquals(Score.nullScore(), score(candidateToJustMismatch, subjectToJustMismatch));
    }

}
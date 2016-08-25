package org.atlasapi.equiv.scorers;

import org.atlasapi.equiv.results.description.DefaultDescription;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.media.entity.Item;

import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import static org.junit.Assert.*;

public class DescriptionMatchingScorerTest {

    private DescriptionMatchingScorer scorer;

    public DescriptionMatchingScorerTest() {
        this.scorer = DescriptionMatchingScorer.makeScorer();
    }

    private Score score(Item subject, Item candidate) {
        DefaultDescription desc = new DefaultDescription();
        ScoredCandidates<Item> scores = scorer.score(subject, ImmutableSet.of(candidate), desc);
        return scores.candidates().get(candidate);
    }

    @Test
    public void testMatch() {
        Item subject = new Item();
        Item candidate = new Item();
        subject.setDescription("Football related content and Manchester United talk");
        candidate.setDescription("Manchester united play Football");
        assertEquals(Score.ONE, score(candidate, subject));
        assertEquals(Score.ONE, score(subject, candidate));
    }

    @Test
    public void testMismatch() {
        Item subject = new Item();
        Item candidate = new Item();
        subject.setDescription("Football related content and Manchester United talk");
        candidate.setDescription("Something nothing Actually related");
        assertEquals(Score.nullScore(), score(candidate, subject));
        assertEquals(Score.nullScore(), score(subject, candidate));
    }

    @Test
    public void testCapitals() {
        Item subject = new Item();
        Item candidate = new Item();
        subject.setDescription("Football related content and Manchester United talk");
        candidate.setDescription("football related content and manchester united talk");
        assertEquals(Score.nullScore(), score(candidate, subject));
        assertEquals(Score.nullScore(), score(subject, candidate));
    }

}
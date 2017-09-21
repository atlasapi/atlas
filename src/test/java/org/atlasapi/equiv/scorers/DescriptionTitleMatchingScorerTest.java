package org.atlasapi.equiv.scorers;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.atlasapi.equiv.results.description.DefaultDescription;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.equiv.scorers.DescriptionTitleMatchingScorer;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResults;
import org.atlasapi.media.entity.Item;

import com.google.common.collect.ImmutableSet;
import org.junit.Test;
import scala.actors.threadpool.Arrays;

import static org.junit.Assert.assertEquals;


public class DescriptionTitleMatchingScorerTest {

    private DescriptionTitleMatchingScorer scorer;

    public DescriptionTitleMatchingScorerTest() {
        this.scorer = new DescriptionTitleMatchingScorer();
    }

    @Test
    public void testMatch() {
        Item subject = new Item();
        subject.setTitle("This is the News");
        Item candidate = new Item();
        candidate.setDescription("This is the News which discusses news related stuff");
        assertEquals(Score.ONE, score(subject, candidate));
        assertEquals(Score.ONE, score(candidate, subject));
    }

    @Test
    public void testMoreDifficultMismatch() {
        Item subject = new Item();
        subject.setTitle("Something not football Related");
        Item candidate = new Item();
        candidate.setDescription("something not football related Either");
        assertEquals(Score.nullScore(), score(subject, candidate));
        assertEquals(Score.nullScore(), score(candidate, subject));
    }

    @Test
    public void testCapitals() {
        Item subject = new Item();
        subject.setTitle("something lower case");
        Item candidate = new Item();
        candidate.setDescription("something also lower case");
        assertEquals(Score.nullScore(), score(subject, candidate));
        assertEquals(Score.nullScore(), score(candidate, subject));

        Item subjectTwo = new Item();
        subjectTwo.setTitle("something lower case");
        Item candidateTwo = new Item();
        candidateTwo.setDescription("Something Not Lower Case");
        assertEquals(Score.ONE, score(subjectTwo, candidateTwo));
        assertEquals(Score.ONE, score(candidateTwo, subjectTwo));

        assertEquals(Score.ONE, score(subject, candidateTwo));
        assertEquals(Score.nullScore(), score(subjectTwo, candidate));
    }

    @Test
    public void testMismatch() {
        Item subject = new Item();
        subject.setTitle("Football Madness");
        Item candidate = new Item();
        candidate.setDescription("Some unrelated Stuff that doesn't Mention what's in the title");
        assertEquals(Score.nullScore(), score(subject, candidate));
        assertEquals(Score.nullScore(), score(candidate, subject));
    }

    @Test
    public void testNulls() {
        Item subject = new Item();
        Item candidate = new Item();
        assertEquals(Score.nullScore(), score(subject, candidate));
        assertEquals(Score.nullScore(), score(candidate, subject));
    }

    @Test
    public void testDivisionFactor() {
        Item subject = new Item();
        Item candidate = new Item();
        subject.setTitle("Word");
        candidate.setDescription("Word and Some Other Words That Are Different");
        assertEquals(Score.ONE, score(subject, candidate));
        assertEquals(Score.ONE, score(candidate, subject));
    }

    @Test
    public void testFailDivisonFactor() {
        Item subject = new Item();
        Item candidate = new Item();
        subject.setTitle("Three small words");
        candidate.setDescription("Three is the only Word Repeated");
        assertEquals(Score.nullScore(), score(subject, candidate));
        assertEquals(Score.nullScore(), score(candidate, subject));
    }

    @Test
    public void testPunctuation() {
        Item subject = new Item();
        Item candidate = new Item();
        subject.setTitle("Dr. Punctuation, and the Test");
        candidate.setDescription("Dr Punctuation and the test");
        assertEquals(Score.ONE, score(subject, candidate));
        assertEquals(Score.ONE, score(candidate, subject));

        Item subjectTwo = new Item();
        Item candidateTwo = new Item();
        subjectTwo.setDescription("Dr. Punctuation, and the Test");
        candidateTwo.setTitle("Dr Punctuation and the test");
        assertEquals(Score.ONE, score(subjectTwo, candidateTwo));
        assertEquals(Score.ONE, score(candidateTwo, subjectTwo));
    }

    @Test
    public void testNumbers() {
        Item subject = new Item();
        Item candidate = new Item();
        subject.setTitle("Dr. Punctuation, Test 2");
        candidate.setDescription("Dr Punctuation and the Test 2");
        assertEquals(Score.ONE, score(subject, candidate));
        assertEquals(Score.ONE, score(candidate, subject));
    }

    private Score score(Item subject, Item candidate) {
        DefaultDescription desc = new DefaultDescription();
        ScoredCandidates<Item> scores = scorer.score(
                subject,
                ImmutableSet.of(candidate),
                desc,
                EquivToTelescopeResults.create("id", "publisher")
        );
        return scores.candidates().get(candidate);
    }
}
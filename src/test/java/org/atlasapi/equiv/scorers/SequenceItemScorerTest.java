package org.atlasapi.equiv.scorers;

import static org.atlasapi.equiv.results.scores.Score.NULL_SCORE;
import static org.atlasapi.equiv.results.scores.Score.ONE;
import static org.junit.Assert.assertEquals;

import java.util.Set;

import org.atlasapi.equiv.results.description.DefaultDescription;
import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.ParentRef;
import org.atlasapi.media.entity.Publisher;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;

public class SequenceItemScorerTest {

    private final SequenceItemScorer scorer = new SequenceItemScorer();
    private final ResultDescription desc = new DefaultDescription();

    @Test
    public void testScoresNullWhenSubjectNotEpisode() {
        Item subject = new Item("item", "item", Publisher.BBC);
        Episode candidate = episode("episode", 4, 5);

        ScoredCandidates<Item> scores = scorer.score(subject, set(candidate), desc);

        assertEquals("should score null if subject not episode",
            NULL_SCORE, scores.candidates().get(candidate));
    }

    @Test
    public void testScoresNullWhenCandidateNotEpisode() {
        Episode subject = episode("subject", 5, 4);
        Item candidate = new Item("item", "item", Publisher.BBC);

        ScoredCandidates<Item> scores = scorer.score(subject, set(candidate), desc);

        assertEquals("should score null if candidate not episode",
            NULL_SCORE, scores.candidates().get(candidate));
    }

    @Test
    public void testScoresNullWhenEpisodeAndSeriesNumbersAbsent() {
        Episode subject = episode("subject", null, null);
        Episode candidate = episode("candidate", null, null);
        
        ScoredCandidates<Item> scores = scorer.score(subject, set(candidate), desc);
        
        assertEquals("should score null if episode and series number absent",
            NULL_SCORE, scores.candidates().get(candidate));
    }

    @Test
    public void testScoresNullWhenEpisodeNumbersAbsentAndSeriesNumbersMatch() {
        Episode subject = episode("subject", 6, null);
        Episode candidate = episode("candidate", 6, null);
        
        ScoredCandidates<Item> scores = scorer.score(subject, set(candidate), desc);
        
        assertEquals("should score null if episode number absent and series number match",
            NULL_SCORE, scores.candidates().get(candidate));
    }

    @Test
    public void testScoresNullWhenEpisodeNumbersAbsentAndSeriesNumbersDiffer() {
        Episode subject = episode("subject", 5, null);
        Episode candidate = episode("candidate", 4, null);
        
        ScoredCandidates<Item> scores = scorer.score(subject, set(candidate), desc);
        
        assertEquals("should score null if episode number absent and series number differs",
            NULL_SCORE, scores.candidates().get(candidate));
    }

    @Test
    public void testScoresNullWhenSeriesNumbersDiffer() {
        Episode subject = episode("subject", 5, 4);
        Episode candidate = episode("candidate", 4, 4);

        ScoredCandidates<Item> scores = scorer.score(subject, set(candidate), desc);

        assertEquals("should score null if series number differs",
            NULL_SCORE, scores.candidates().get(candidate));
    }

    @Test
    public void testScoresNullWhenEpisodeNumbersDiffer() {
        Episode subject = episode("subject", 5, 4);
        Episode candidate = episode("candidate", 5, 6);

        ScoredCandidates<Item> scores = scorer.score(subject, set(candidate), desc);

        assertEquals("should score null if episode number differs",
            NULL_SCORE, scores.candidates().get(candidate));
    }


    @Test
    public void testScoresNullWhenSeriesNumbersAbsentAndEpisodeNumbersDiffer() {
        Episode subject = episode("subject", null, 5);
        Episode candidate = episode("candidate", null, 6);
        
        ScoredCandidates<Item> scores = scorer.score(subject, set(candidate), desc);
        
        assertEquals("should score null if series number absent and episode numbers differ",
            NULL_SCORE, scores.candidates().get(candidate));
    }

    @Test
    public void testScoresNullWhenSeriesNumbersAndEpisodeNumbersDiffer() {
        Episode subject = episode("subject", 4, 5);
        Episode candidate = episode("candidate", 8, 6);
        
        ScoredCandidates<Item> scores = scorer.score(subject, set(candidate), desc);
        
        assertEquals("should score null if series number and episode numbers differ",
            NULL_SCORE, scores.candidates().get(candidate));
    }

    @Test
    public void testScoresOneWhenSeriesNumbersAbsentAndEpisodeNumbersMatch() {
        Episode subject = episode("subject", null, 6);
        Episode candidate = episode("candidate", null, 6);
        
        ScoredCandidates<Item> scores = scorer.score(subject, set(candidate), desc);
        
        assertEquals("should score one if series number absent and episode numbers match",
            ONE, scores.candidates().get(candidate));
    }

    @Test
    public void testScoresOneWhenSeriesNumbersAndEpisodeNumbersMatch() {
        Episode subject = episode("subject", 5, 6);
        Episode candidate = episode("candidate", 5, 6);
        
        ScoredCandidates<Item> scores = scorer.score(subject, set(candidate), desc);
        
        assertEquals("should score one if series number absent and episode numbers match",
            ONE, scores.candidates().get(candidate));
    }
    
    @Test
    public void testScoresNullIfSubjectChildOfTopLevelSeriesAndCandidateNot() {
        Episode subject = episode("subject", 5, 6);
        subject.setSeriesRef(subject.getContainer());
        Episode candidate = episode("candidate", 5, 6);
        
        ScoredCandidates<Item> scores = scorer.score(subject, set(candidate), desc);
        
        assertEquals("should score null if subject child of top-level series and candidate not",
            NULL_SCORE, scores.candidates().get(candidate));
        
    }

    @Test
    public void testScoresNullIfCandidateChildOfTopLevelSeriesAndSubjectNot() {
        Episode subject = episode("subject", 5, 6);
        Episode candidate = episode("candidate", 5, 6);
        candidate.setSeriesRef(candidate.getContainer());
        
        ScoredCandidates<Item> scores = scorer.score(subject, set(candidate), desc);
        
        assertEquals("should score null if candidate child of top-level series and subject not",
                NULL_SCORE, scores.candidates().get(candidate));
        
    }

    private Episode episode(String uri, Integer seriesNumber, Integer episodeNumber) {
        Episode candidate = new Episode(uri, uri, Publisher.BBC);
        candidate.setParentRef(new ParentRef("b"+uri));
        candidate.setSeriesRef(new ParentRef("s"+uri));
        candidate.setSeriesNumber(seriesNumber);
        candidate.setEpisodeNumber(episodeNumber);
        return candidate;
    }

    private final <T> Set<T> set(T... ts) {
        return ImmutableSet.copyOf(ts);
    }

}

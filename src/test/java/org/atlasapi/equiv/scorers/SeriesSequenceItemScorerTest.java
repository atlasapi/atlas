package org.atlasapi.equiv.scorers;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

import java.util.Set;

import org.atlasapi.equiv.results.description.DefaultDescription;
import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.ParentRef;
import org.atlasapi.media.entity.Publisher;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;


public class SeriesSequenceItemScorerTest {

    private final SeriesSequenceItemScorer scorer = new SeriesSequenceItemScorer();
    private final ResultDescription desc = new DefaultDescription();
    
    @Test
    public void testEpisodesWithSameSeriesNumberScoreOne() {
        checkScore(episode("s", 5, 5), episode("c", 5, 5), Score.ONE);
    }
    
    @Test
    public void testEpisodesWithDifferentSeriesNumberScoreNegativeOne() {
        checkScore(episode("s", 5, 5), episode("c", 5600, 5), Score.valueOf(-1.0));
    }
    
    @Test
    public void testSubjectItemScoresNull() {
        checkScore(item("s"), episode("c", 5, 5), Score.NULL_SCORE);
    }
    
    @Test
    public void testCandidateItemScoresNull() {
        checkScore(episode("s", 5, 5), item("c"), Score.NULL_SCORE);
    }
    
    @Test
    public void testEpisodesWithoutSeriesNumberScoreNull() {
        checkScore(episode("s", null, 5), episode("c",null,5), Score.NULL_SCORE);
    }
    
    @Test
    /*
     * A subject dataset may not have series and episode numbers, since it
     * may not have hierarchy information. This should not stop it matching
     * to a hierarchical dataset. 
     */
    public void testSubjectWithNoSeriesAndEpisodeNumberScoresNull() {
        checkScore(episode("s", null, null), episode("c", 5, 5), Score.nullScore());
    }

    private void checkScore(Item subject, Item candidate, Score expectedScore) {
        ScoredCandidates<Item> scores = scorer.score(subject, set(candidate), desc);
        assertThat(scores.candidates().get(candidate), is(expectedScore));
    }

    private Item item(String uri) {
        Item item = new Item(uri, uri, Publisher.BBC);
        item.setParentRef(new ParentRef("b"+uri));
        return item;
    }
    
    private Episode episode(String uri, Integer seriesNumber, Integer episodeNumber) {
        Episode episode = new Episode(uri, uri, Publisher.BBC);
        episode.setParentRef(new ParentRef("b"+uri));
        episode.setSeriesRef(new ParentRef("s"+uri));
        episode.setSeriesNumber(seriesNumber);
        episode.setEpisodeNumber(episodeNumber);
        return episode;
    }

    private final <T> Set<T> set(T... ts) {
        return ImmutableSet.copyOf(ts);
    }
    
}

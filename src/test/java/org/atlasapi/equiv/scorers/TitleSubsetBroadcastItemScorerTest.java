package org.atlasapi.equiv.scorers;

import static org.junit.Assert.assertEquals;

import org.atlasapi.equiv.results.description.DefaultDescription;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.testing.StubContentResolver;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;

public class TitleSubsetBroadcastItemScorerTest {

    private final ContentResolver resolver = new StubContentResolver();
    private final TitleSubsetBroadcastItemScorer scorer
        = new TitleSubsetBroadcastItemScorer(resolver, Score.nullScore(), 100);
    
    @Test
    public void testMatches() {
        assertEquals(Score.ONE, score(
            itemWithTitle("The Ren & Stimpy Show"), 
            itemWithTitle("Ren and Stimpy!")
        ));
        assertEquals(Score.ONE, score(
            itemWithTitle("New: Uncle"), 
            itemWithTitle("Uncle")
        ));
        assertEquals(Score.ONE, score(
            itemWithTitle("Doctor Who?"), 
            itemWithTitle("Doctor Who Confidential")
        ));
        assertEquals(Score.ONE, score(
                itemWithTitle("Power Rangers: R.P.M."),
                itemWithTitle("Power Rangers RPM")
        ));
        assertEquals(Score.ONE, score(
                itemWithTitle("Power - Rangers: R.P.M.!!"),
                itemWithTitle("Power Rangers RPM")
        ));
    }
    
    @Test
    public void testMisMatches() {
        assertEquals(Score.nullScore(), score(
            itemWithTitle("Title Which Has Only One Word In Common With The Other"), 
            itemWithTitle("Title That Contains Single Utterance In Subject")
        ));
    }

    private Score score(Item subject, Item candidate) {
        DefaultDescription desc = new DefaultDescription();
        ScoredCandidates<Item> scores = scorer.score(subject, ImmutableSet.of(candidate), desc);
        return scores.candidates().get(candidate);
    }

    private Item itemWithTitle(String title) {
        Item item = new Item(title, title, Publisher.PA);
        item.setTitle(title);
        return item;
    }

}

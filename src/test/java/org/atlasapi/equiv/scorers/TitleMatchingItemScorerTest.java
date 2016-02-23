package org.atlasapi.equiv.scorers;

import static com.google.common.collect.ImmutableSet.of;
import junit.framework.TestCase;

import org.atlasapi.equiv.results.description.DefaultDescription;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.equiv.scorers.TitleMatchingItemScorer;
import org.atlasapi.equiv.scorers.TitleMatchingItemScorer.TitleType;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.junit.Test;

import com.google.common.collect.Iterables;

public class TitleMatchingItemScorerTest extends TestCase {

    private final TitleMatchingItemScorer scorer = new TitleMatchingItemScorer();

    @Test
    public void testTitleTyping() {
        
       assertEquals(TitleType.DATE, TitleType.titleTypeOf(itemWithTitle("09/10/2011")));
       assertEquals(TitleType.DATE, TitleType.titleTypeOf(itemWithTitle("9/10/2011")));
       assertEquals(TitleType.DATE, TitleType.titleTypeOf(itemWithTitle("09/1/2011")));
       assertEquals(TitleType.DATE, TitleType.titleTypeOf(itemWithTitle("1/1/2011")));
       assertEquals(TitleType.DATE, TitleType.titleTypeOf(itemWithTitle("1/1/11")));
       
       assertEquals(TitleType.SEQUENCE, TitleType.titleTypeOf(itemWithTitle("Episode 1")));
       assertEquals(TitleType.SEQUENCE, TitleType.titleTypeOf(itemWithTitle("Episode: 1")));
       assertEquals(TitleType.SEQUENCE, TitleType.titleTypeOf(itemWithTitle("Episode - 1")));
       assertEquals(TitleType.SEQUENCE, TitleType.titleTypeOf(itemWithTitle("episode  1")));
       assertEquals(TitleType.SEQUENCE, TitleType.titleTypeOf(itemWithTitle("episode 14")));
       
       assertEquals(TitleType.DEFAULT, TitleType.titleTypeOf(itemWithTitle("09/10/20118")));
       assertEquals(TitleType.DEFAULT, TitleType.titleTypeOf(itemWithTitle("009/10/2011")));
       assertEquals(TitleType.DEFAULT, TitleType.titleTypeOf(itemWithTitle("09/100/2011")));
        
    }

    @Test
    public void testGenerateEquivalences() {

        DefaultDescription desc = new DefaultDescription();
        
        score(2.0, scorer.score(itemWithTitle("09/10/2011"), of(itemWithTitle("09/10/2011")), desc));
        
        score(0, scorer.score(itemWithTitle("19/10/2011"), of(itemWithTitle("09/10/2011")), desc));
        score(0, scorer.score(itemWithTitle("Countdown"), of(itemWithTitle("Out of Time")), desc));
        score(0, scorer.score(itemWithTitle("Episode: 3"), of(itemWithTitle("Episode 5")), desc));
        
        score(0, scorer.score(itemWithTitle("19/10/2011"), of(itemWithTitle("Different")), desc));
        score(0, scorer.score(itemWithTitle("Episode 1"), of(itemWithTitle("19/10/2011")), desc));
        score(0, scorer.score(itemWithTitle("Episode 1"), of(itemWithTitle("Different")), desc));
        
    }

    @Test
    public void testSeqTitleTypes() {

        DefaultDescription desc = new DefaultDescription();
        
        score(2, scorer.score(itemWithTitle("Kinross"), of(itemWithTitle("2. Kinross")), desc));
        score(2, scorer.score(itemWithTitle("Kinross"), of(itemWithTitle("2: Kinross")), desc));
        score(2, scorer.score(itemWithTitle("Kinross"), of(itemWithTitle("2 - Kinross")), desc));
        score(0, scorer.score(itemWithTitle("Kinross"), of(itemWithTitle("2. Different")), desc));
        
    }
    
    @Test
    public void testMatchingWithAmpersands() {
        
        DefaultDescription desc = new DefaultDescription();

        score(2, scorer.score(itemWithTitle("Rosencrantz & Guildenstern Are Dead"), of(itemWithTitle("Rosencrantz and Guildenstern Are Dead")), desc));
        score(2, scorer.score(itemWithTitle("Bill & Ben"), of(itemWithTitle("2. Bill and Ben")), desc));
        score(0, scorer.score(itemWithTitle("B&Q"), of(itemWithTitle("BandQ")), desc));

    }

    @Test
    public void testNormalizeBackToBacksSpacing() {

        DefaultDescription desc = new DefaultDescription();

        score(2, scorer.score(itemWithTitle("Foo / Bar"), of(itemWithTitle("Foo/Bar")), desc));
    }
    
    @Test
    public void testMatchingWithThePrefix() {
        DefaultDescription desc = new DefaultDescription();

        score(2, scorer.score(itemWithTitle("Sports"), of(itemWithTitle("Live Sports")), desc));
        score(2, scorer.score(itemWithTitle("The Great Escape"), of(itemWithTitle("Great Escape")), desc));
        score(2, scorer.score(itemWithTitle("the Great Escape"), of(itemWithTitle("Great Escape")), desc));
        score(0, scorer.score(itemWithTitle("Theatreland"), of(itemWithTitle("The atreland")), desc));
        score(0, scorer.score(itemWithTitle("theatreland"), of(itemWithTitle("the atreland")), desc));
        score(0, scorer.score(itemWithTitle("liveandnotlive live"), of(itemWithTitle("live")), desc));

    }
    
    @Test
    public void testMatchingSports() {
        DefaultDescription desc = new DefaultDescription();
        
        // score(1, scorer.score(itemWithTitle("Live B' Monchengladbach v Man City"), of(itemWithTitle("Borussia Moenchengladbach v Manchester City")), desc));
        score(2, scorer.score(itemWithTitle("Live Porto v Chelsea"), of(itemWithTitle("FC Porto v Chelsea")), desc));
        score(2, scorer.score(itemWithTitle("Live Maccabi Tel-Aviv v D' Kiev"), of(itemWithTitle("Maccabi Tel Aviv v Dynamo Kiev")), desc));
        score(2, scorer.score(itemWithTitle("Live Maccabi Tel-Aviv v D' Kiev"), of(itemWithTitle("Maccabi Tel Aviv v D' Kiev")), desc));
        score(2, scorer.score(itemWithTitle("Live Maccabi Tel-Aviv v Dynamo Kiev"), of(itemWithTitle("Maccabi Tel Aviv v D' Kiev")), desc));
    }

    @Test
    public void testMatchingWithApostrophe() {
        //This test case covers cases when non-abbrivating apostrophe is used in the end of the word
        // like "Girls' Night In" with "Girls' Night In"
        DefaultDescription desc = new DefaultDescription();
        score(2, scorer.score(itemWithTitle("Girls' Night In"), of(itemWithTitle("Girls' Night In")), desc));
        score(2, scorer.score(itemWithTitle("Girls Night In"), of(itemWithTitle("Girls' Night In")), desc));
        score(2, scorer.score(itemWithTitle("Girls' Night In"), of(itemWithTitle("Girls Night In")), desc));
        score(2, scorer.score(itemWithTitle("Girls Night In"), of(itemWithTitle("Girls Night In")), desc));
    }

    @Test
    public void testMatchingWithApostropheWithinWord() {
        DefaultDescription desc = new DefaultDescription();
        //This test case covers cases when non-abbrivating apostrophe is used within a word
        // like Charlies Big Catch" with "Charlie's Big Catch"
        score(2, scorer.score(itemWithTitle("Charlies Big Catch"), of(itemWithTitle("Charlie's Big Catch")), desc));
        score(2, scorer.score(itemWithTitle("Charlie's Big Catch"), of(itemWithTitle("Charlie's Big Catch")), desc));
        score(2, scorer.score(itemWithTitle("Charlie's Big Catch"), of(itemWithTitle("Charlies Big Catch")), desc));
        score(2, scorer.score(itemWithTitle("Charlies Big Catch"), of(itemWithTitle("Charlies Big Catch")), desc));
        score(2, scorer.score(itemWithTitle("C'harlies Big Catch"), of(itemWithTitle("Charlies Big Catch")), desc));
        score(2, scorer.score(itemWithTitle("Charlies Big Catch"), of(itemWithTitle("C'harlies Big Catch")), desc));
        score(2, scorer.score(itemWithTitle("C'harlies Big Catch"), of(itemWithTitle("C'harlies Big Catch")), desc));
        score(0, scorer.score(itemWithTitle("C'harlie Big Catch"), of(itemWithTitle("C'harlies Big Catch")), desc));
    }
    
    private void score(double expected, ScoredCandidates<Item> scores) {
        Score value = Iterables.getOnlyElement(scores.candidates().entrySet()).getValue();
        assertTrue(String.format("expected %s got %s", expected, value), value.equals(expected > 0 ? Score.valueOf(expected) : Score.NULL_SCORE));
    }

    private Item itemWithTitle(String title) {
        Item item = new Item("uri","curie",Publisher.BBC);
        item.setTitle(title);
        return item;
    }

}

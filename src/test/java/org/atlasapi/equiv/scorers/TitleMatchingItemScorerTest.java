package org.atlasapi.equiv.scorers;

import com.google.common.collect.Iterables;
import junit.framework.TestCase;
import org.atlasapi.equiv.results.description.DefaultDescription;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.equiv.scorers.TitleMatchingItemScorer.TitleType;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResult;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.junit.Test;

import static com.google.common.collect.ImmutableSet.of;

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
        EquivToTelescopeResult equivToTelescopeResult = EquivToTelescopeResult.create(
                "id",
                "publisher"
        );

        score(2.0, scorer.score(itemWithTitle("09/10/2011"), of(itemWithTitle("09/10/2011")), desc, equivToTelescopeResult));
        
        score(0, scorer.score(itemWithTitle("19/10/2011"), of(itemWithTitle("09/10/2011")), desc, equivToTelescopeResult));
        score(0, scorer.score(itemWithTitle("Countdown"), of(itemWithTitle("Out of Time")), desc, equivToTelescopeResult));
        score(0, scorer.score(itemWithTitle("Episode: 3"), of(itemWithTitle("Episode 5")), desc, equivToTelescopeResult));
        
        score(0, scorer.score(itemWithTitle("19/10/2011"), of(itemWithTitle("Different")), desc, equivToTelescopeResult));
        score(0, scorer.score(itemWithTitle("Episode 1"), of(itemWithTitle("19/10/2011")), desc, equivToTelescopeResult));
        score(0, scorer.score(itemWithTitle("Episode 1"), of(itemWithTitle("Different")), desc, equivToTelescopeResult));
        
    }

    @Test
    public void testSeqTitleTypes() {

        DefaultDescription desc = new DefaultDescription();
        EquivToTelescopeResult equivToTelescopeResult = EquivToTelescopeResult.create(
                "id",
                "publisher"
        );
        
        score(2, scorer.score(itemWithTitle("Kinross"), of(itemWithTitle("2. Kinross")), desc, equivToTelescopeResult));
        score(2, scorer.score(itemWithTitle("Kinross"), of(itemWithTitle("2: Kinross")), desc, equivToTelescopeResult));
        score(2, scorer.score(itemWithTitle("Kinross"), of(itemWithTitle("2 - Kinross")), desc, equivToTelescopeResult));
        score(0, scorer.score(itemWithTitle("Kinross"), of(itemWithTitle("2. Different")), desc, equivToTelescopeResult));
        
    }

    @Test
    public void testVsShouldNotBreakIfV() {
        DefaultDescription desc = new DefaultDescription();

        score(2, scorer.score(
                itemWithTitle("Gabriel Iglesias vs. Randy Couture"),
                of(itemWithTitle("Gabriel Iglesias v Randy Couture")

                        ), desc, EquivToTelescopeResult.create(
                        "id",
                        "publisher"
                )));
    }

    @Test
    public void testMatchingWithAmpersands() {
        
        DefaultDescription desc = new DefaultDescription();
        EquivToTelescopeResult equivToTelescopeResult = EquivToTelescopeResult.create(
                "id",
                "publisher"
        );

        score(2, scorer.score(itemWithTitle("Rosencrantz & Guildenstern Are Dead"), of(itemWithTitle("Rosencrantz and Guildenstern Are Dead")), desc, equivToTelescopeResult));
        score(2, scorer.score(itemWithTitle("Bill & Ben"), of(itemWithTitle("2. Bill and Ben")), desc, equivToTelescopeResult));
        score(0, scorer.score(itemWithTitle("B&Q"), of(itemWithTitle("BandQ")), desc, equivToTelescopeResult));

    }

    @Test
    public void testNormalizeBackToBacksSpacing() {

        DefaultDescription desc = new DefaultDescription();

        score(2, scorer.score(
                itemWithTitle("Foo / Bar"),
                of(itemWithTitle("Foo/Bar")),
                desc,
                EquivToTelescopeResult.create("id", "publisher"))
        );
    }
    
    @Test
    public void testMatchingWithThePrefix() {
        DefaultDescription desc = new DefaultDescription();
        EquivToTelescopeResult equivToTelescopeResult = EquivToTelescopeResult.create(
                "id",
                "publisher"
        );

        score(2, scorer.score(
                itemWithTitle("Funny People"),
                of(itemWithTitle("Funny People (Unrated)")),
                desc,
                equivToTelescopeResult
        ));
        score(2, scorer.score(
                itemWithTitle("Unrated People"),
                of(itemWithTitle("Unrated People")),
                desc,
                equivToTelescopeResult
        ));
        score(2, scorer.score(
                itemWithTitle("Sports"),
                of(itemWithTitle("Live Sports")),
                desc,
                equivToTelescopeResult
        ));
        score(2, scorer.score(
                itemWithTitle("The Great Escape"),
                of(itemWithTitle("Great Escape")),
                desc,
                equivToTelescopeResult
        ));
        score(2, scorer.score(
                itemWithTitle("the Great Escape"),
                of(itemWithTitle("Great Escape")),
                desc,
                equivToTelescopeResult
        ));
        score(0, scorer.score(
                itemWithTitle("Theatreland"),
                of(itemWithTitle("The atreland")),
                desc,
                equivToTelescopeResult
        ));
        score(0, scorer.score(
                itemWithTitle("theatreland"),
                of(itemWithTitle("the atreland")),
                desc,
                equivToTelescopeResult
        ));
        score(0, scorer.score(
                itemWithTitle("liveandnotlive live"),
                of(itemWithTitle("live")),
                desc,
                equivToTelescopeResult
        ));
        score(0, scorer.score(
                itemWithTitle("Funny People"),
                of(itemWithTitle("Funny People Unrated")),
                desc,
                equivToTelescopeResult
        ));
    }
    
    @Test
    public void testMatchingSports() {
        DefaultDescription desc = new DefaultDescription();
        EquivToTelescopeResult equivToTelescopeResult = EquivToTelescopeResult.create(
                "id",
                "publisher"
        );
        
        // score(1, scorer.score(itemWithTitle("Live B' Monchengladbach v Man City"),
        // of(itemWithTitle("Borussia Moenchengladbach v Manchester City")), desc));
        score(2, scorer.score(
                itemWithTitle("Live Porto v Chelsea"),
                of(itemWithTitle("FC Porto v Chelsea")),
                desc,
                equivToTelescopeResult
        ));
        score(2, scorer.score(
                itemWithTitle("Live Maccabi Tel-Aviv v D' Kiev"),
                of(itemWithTitle("Maccabi Tel Aviv v Dynamo Kiev")),
                desc,
                equivToTelescopeResult
        ));
        score(2, scorer.score(
                itemWithTitle("Live Maccabi Tel-Aviv v D' Kiev"),
                of(itemWithTitle("Maccabi Tel Aviv v D' Kiev")),
                desc,
                equivToTelescopeResult
        ));
        score(2, scorer.score(
                itemWithTitle("Live Maccabi Tel-Aviv v Dynamo Kiev"),
                of(itemWithTitle("Maccabi Tel Aviv v D' Kiev")),
                desc,
                equivToTelescopeResult
        ));
    }

    @Test
    public void testMatchingWithApostrophe() {
        //This test case covers cases when non-abbrivating apostrophe is used in the end of the word
        // like "Girls' Night In" with "Girls' Night In"
        DefaultDescription desc = new DefaultDescription();
        EquivToTelescopeResult equivToTelescopeResult = EquivToTelescopeResult.create(
                "id",
                "publisher"
        );

        score(2, scorer.score(
                itemWithTitle("Girls' Night In"),
                of(itemWithTitle("Girls' Night In")),
                desc,
                equivToTelescopeResult
        ));
        score(2, scorer.score(
                itemWithTitle("Girls Night In"),
                of(itemWithTitle("Girls' Night In")),
                desc,
                equivToTelescopeResult
        ));
        score(2, scorer.score(
                itemWithTitle("Girls' Night In"),
                of(itemWithTitle("Girls Night In")),
                desc,
                equivToTelescopeResult
        ));
        score(2, scorer.score(
                itemWithTitle("Girls Night In"),
                of(itemWithTitle("Girls Night In")),
                desc,
                equivToTelescopeResult
        ));
    }

    @Test
    public void testMatchingWithPunctuation() {
        DefaultDescription desc = new DefaultDescription();
        EquivToTelescopeResult equivToTelescopeResult = EquivToTelescopeResult.create(
                "id",
                "publisher"
        );

        score(2, scorer.score(
                itemWithTitle("48 hrs"),
                of(itemWithTitle("48 HRS.")),
                desc,
                equivToTelescopeResult
        ));
        score(2, scorer.score(
                itemWithTitle("The 7:51"),
                of(itemWithTitle("The 7.51")),
                desc,
                equivToTelescopeResult
        ));
        score(2, scorer.score(
                itemWithTitle("Mr. & Mrs. Smith"),
                of(itemWithTitle("Mr & Mrs Smith")),
                desc,
                equivToTelescopeResult
        ));
        score(2, scorer.score(
                itemWithTitle("The way, way back"),
                of(itemWithTitle("The way way back")),
                desc,
                equivToTelescopeResult
        ));
        score(2, scorer.score(
                itemWithTitle("The weekend"),
                of(itemWithTitle("The week-end")),
                desc,
                equivToTelescopeResult
        ));
        score(2, scorer.score(
                itemWithTitle("'Allo 'Allo!"),
                of(itemWithTitle("Allo Allo")),
                desc,
                equivToTelescopeResult
        ));
    }

    @Test
    public void testMatchingTitlesWithYearsInTitles() {
        DefaultDescription desc = new DefaultDescription();
        EquivToTelescopeResult equivToTelescopeResult = EquivToTelescopeResult.create(
                "id",
                "publisher"
        );

        score(2, scorer.score(
                itemWithTitle("Cold Comes the Night (2013)"),
                of(itemWithTitle("Cold Comes the Night")),
                desc,
                equivToTelescopeResult
        ));
        score(2, scorer.score(
                itemWithTitle("Get Carter (2013)"),
                of(itemWithTitle("Get Carter")),
                desc,
                equivToTelescopeResult
        ));
        score(0, scorer.score(
                itemWithTitle("Space Odessey 2013"),
                of(itemWithTitle("Space Odessey")),
                desc,
                equivToTelescopeResult
        ));
    }


    @Test
    public void testMatchingWithApostropheWithinWord() {
        DefaultDescription desc = new DefaultDescription();
        //This test case covers cases when non-abbrivating apostrophe is used within a word
        // like Charlies Big Catch" with "Charlie's Big Catch"

        EquivToTelescopeResult equivToTelescopeResult = EquivToTelescopeResult.create(
                "id",
                "publisher"
        );

        score(2, scorer.score(
                itemWithTitle("Charlies Big Catch"),
                of(itemWithTitle("Charlie's Big Catch")),
                desc,
                equivToTelescopeResult
        ));
        score(2, scorer.score(
                itemWithTitle("Charlie's Big Catch"),
                of(itemWithTitle("Charlie's Big Catch")),
                desc,
                equivToTelescopeResult
        ));
        score(2, scorer.score(
                itemWithTitle("Charlie's Big Catch"),
                of(itemWithTitle("Charlies Big Catch")),
                desc,
                equivToTelescopeResult
        ));
        score(2, scorer.score(
                itemWithTitle("Charlies Big Catch"),
                of(itemWithTitle("Charlies Big Catch")),
                desc,
                equivToTelescopeResult
        ));
        score(2, scorer.score(
                itemWithTitle("C'harlies Big Catch"),
                of(itemWithTitle("Charlies Big Catch")),
                desc,
                equivToTelescopeResult
        ));
        score(2, scorer.score(
                itemWithTitle("Charlies Big Catch"),
                of(itemWithTitle("C'harlies Big Catch")),
                desc,
                equivToTelescopeResult
        ));
        score(2, scorer.score(
                itemWithTitle("C'harlies Big Catch"),
                of(itemWithTitle("C'harlies Big Catch")),
                desc,
                equivToTelescopeResult
        ));
        score(0, scorer.score(
                itemWithTitle("C'harlie Big Catch"),
                of(itemWithTitle("C'harlies Big Catch")),
                desc,
                equivToTelescopeResult
        ));
    }

    @Test
    public void testMatchingWithDifferentSpacing() {
        DefaultDescription desc = new DefaultDescription();
        EquivToTelescopeResult equivToTelescopeResult = EquivToTelescopeResult.create(
                "id",
                "publisher"
        );

        score(2, scorer.score(
                itemWithTitle("HouseBusters"),
                of(itemWithTitle("House Busters")),
                desc,
                equivToTelescopeResult
        ));
        score(2, scorer.score(
                itemWithTitle("House  Busters  "),
                of(itemWithTitle("  House Busters")),
                desc,
                equivToTelescopeResult
        ));
        score(2, scorer.score(
                itemWithTitle("Iron Man 3"),
                of(itemWithTitle("IronMan 3")),
                desc,
                equivToTelescopeResult
        ));
    }
    
    @Test
    public void testMachingWithDifferentNumberingSystem(){
        //do not change romans anywhere but the end of a sentence
        DefaultDescription desc = new DefaultDescription();
        EquivToTelescopeResult equivToTelescopeResult = EquivToTelescopeResult.create(
                "id",
                "publisher"
        );

        score(2, scorer.score(
                itemWithTitle("Iron Man 3"),
                of(itemWithTitle("Iron Man III")),
                desc,
                equivToTelescopeResult
        ));
        score(2, scorer.score(
                itemWithTitle("The world and I"),
                of(itemWithTitle("The world and one")),
                desc,
                equivToTelescopeResult
        )); //sideeffect
        score(0, scorer.score(
                itemWithTitle("V for vendetta"),
                of(itemWithTitle("Five for Vendetta")),
                desc,
                equivToTelescopeResult
        ));
        score(0, scorer.score(
                itemWithTitle("Three v Five"),
                of(itemWithTitle("Three Five Five")),
                desc,
                equivToTelescopeResult
        ));
    }
    
    @Test
    public void testMachingWithDifferentEnglish(){
        //american vs brisish english should match
        DefaultDescription desc = new DefaultDescription();
        score(2, scorer.score(
                itemWithTitle("British Harbor"),
                of(itemWithTitle("British Harbour")),
                desc,
                EquivToTelescopeResult.create("id", "publisher"))
        );
    }

    @Test
    public void testMatchingWithDifferentAccents() {
        DefaultDescription desc = new DefaultDescription();
        EquivToTelescopeResult equivToTelescopeResult = EquivToTelescopeResult.create(
                "id",
                "publisher"
        );

        score(2, scorer.score(
                itemWithTitle("En Equilibre"),
                of(itemWithTitle("En équilibre")),
                desc,
                equivToTelescopeResult
        ));
        score(2, scorer.score(
                itemWithTitle("Vie héroïque"),
                of(itemWithTitle("Vie heroique")),
                desc,
                equivToTelescopeResult
        ));
        score(2, scorer.score(
                itemWithTitle("François Cluzet"),
                of(itemWithTitle("Francois Cluzet")),
                desc,
                equivToTelescopeResult
        ));
    }

    @Test
    public void testPartialMatchAfterSemicolon() {
        DefaultDescription desc = new DefaultDescription();
        EquivToTelescopeResult equivToTelescopeResult = EquivToTelescopeResult.create(
                "id",
                "publisher"
        );

        score(1, scorer.score(
                itemWithTitle("Storage Hunters"),
                of(itemWithTitle("Storage Hunters: UK")),
                desc,
                equivToTelescopeResult
        ));
        score(1, scorer.score(
                itemWithTitle("Storage Hunters: UK"),
                of(itemWithTitle("Storage Hunters")),
                desc,
                equivToTelescopeResult
        ));
        score(1, scorer.score(
                itemWithTitle("CSI: NY"),
                of(itemWithTitle("CSI: New York")),
                desc,
                equivToTelescopeResult
        ));
        score(1, scorer.score(
                itemWithTitle("CSI: NY"),
                of(itemWithTitle("CSI")),
                desc,
                equivToTelescopeResult
        ));
    }

    @Test
    public void testForOutOfBoundsException() {
        DefaultDescription desc = new DefaultDescription();
        EquivToTelescopeResult equivToTelescopeResult = EquivToTelescopeResult.create(
                "id",
                "publisher"
        );

        score(0, scorer.score(
                itemWithTitle("Storage Hunters"),
                of(itemWithTitle(":Storage Hunters: UK")),
                desc,
                equivToTelescopeResult
        ));
        score(2, scorer.score(
                itemWithTitle("Storage Hunters"),
                of(itemWithTitle(":Storage Hunters:")),
                desc,
                equivToTelescopeResult
        ));
    }

    @Test
    public void testForMultipleColonsToStart() {
        DefaultDescription desc = new DefaultDescription();
        EquivToTelescopeResult equivToTelescopeResult = EquivToTelescopeResult.create(
                "id",
                "publisher"
        );

        score(0, scorer.score(
                itemWithTitle("Storage Hunters"),
                of(itemWithTitle("::::Storage Hunters: UK")),
                desc,
                equivToTelescopeResult
        ));
        score(2, scorer.score(
                itemWithTitle("Storage Hunters"),
                of(itemWithTitle("::::Storage Hunters")),
                desc,
                equivToTelescopeResult
        ));
    }

    @Test
    public void testForPrefixRemovalBug() {
        DefaultDescription desc = new DefaultDescription();
        EquivToTelescopeResult equivToTelescopeResult = EquivToTelescopeResult.create(
                "id",
                "publisher"
        );

        score(0, scorer.score(
                itemWithTitle("Storage: Bunters"),
                of(itemWithTitle(" 5 : Storage Hunters")),
                desc,
                equivToTelescopeResult
        ));
        score(2, scorer.score(
                itemWithTitle("Storage: Hunters"),
                of(itemWithTitle(" 5 : Storage Hunters")),
                desc,
                equivToTelescopeResult
        ));
    }
    
    private void score(double expected, ScoredCandidates<Item> scores) {
        Score value = Iterables.getOnlyElement(scores.candidates().entrySet()).getValue();
        assertTrue(String.format("expected %s got %s", expected, value), value.equals(expected > 0 ? Score.valueOf(expected) : Score.NULL_SCORE));
    }

    private Item itemWithTitle(String title) {
        Item item = new Item("uri","curie",Publisher.BBC);
        item.setTitle(title);
        item.setYear(2013);
        return item;
    }

}

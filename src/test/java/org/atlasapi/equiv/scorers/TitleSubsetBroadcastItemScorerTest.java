package org.atlasapi.equiv.scorers;

import static org.junit.Assert.assertEquals;

import org.atlasapi.equiv.results.description.DefaultDescription;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Version;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.testing.StubContentResolver;

import com.sun.javafx.scene.control.behavior.ScrollBarBehavior;
import org.joda.time.DateTime;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;

public class TitleSubsetBroadcastItemScorerTest {


    private final BaseBroadcastItemScorer scorer;

    public TitleSubsetBroadcastItemScorerTest(BaseBroadcastItemScorer scorer) {
        this.scorer =  scorer; //new TitleSubsetBroadcastItemScorer(resolver, Score.nullScore(), 80);
    }

    
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

    @Test
    public void testSportsChannelScoreNull() {
        Item item1 = itemWithTitle("Title That Contains Single Utterance In Subject");
        Item item2 = itemWithTitle("Title That Contains Single Utterance In Subject");
        Version version = new Version();
        Broadcast broadcast = new Broadcast("http://ref.atlasapi.org/channels/pressassociation.com/2021", DateTime.now(), DateTime.now());
        version.addBroadcast(broadcast);
        item1.addVersion(version);
        assertEquals(Score.nullScore(), score(
                item1,
                item2
        ));
    }

    @Test
    public void testSymbols() {
        assertEquals(Score.ONE, score(itemWithTitle("Sponge Bob Square Pants"), itemWithTitle("Sponge Bob Square Pants: Episode 1")));
    }

    @Test
    public void testPlurals() {
        assertEquals(Score.ONE, score(itemWithTitle("Motives and Murder"), itemWithTitle("Motives and Murders")));
    }

    @Test
    public void testPunctuation() {
        assertEquals(Score.ONE, score(itemWithTitle("Mr. Robot"), itemWithTitle("Mr Robot")));
    }

    @Test
    public void testAcronyms() {
        assertEquals(Score.ONE, score(itemWithTitle("Title (US)"), itemWithTitle("Title (U.S.A)")));
    }

    @Test
    public void testDescriptionMatching() {
        Item item1 = itemWithTitle("something");
        Item item2 = itemWithTitle("s else");
        item1.setLongDescription("Suicide Squad is a film about several evil villains from DC Comics that get sent off to fight an impossible scenario");
        item2.setLongDescription("the new Comics hit film Suicide Squad is about DC villains sent off to fight a battle to the death");

        assertEquals(Score.ONE, score(item1, item2));
    }

    @Test
    public void testDescriptionMatching2() {
        Item item1 = itemWithTitle("something");
        Item item2 = itemWithTitle("s else");
        item1.setLongDescription("Suicide Squad is a film about several evil villains from DC Comics that get sent off to fight an impossible scenario");
        item2.setLongDescription("The new Comics hit film Suicide Squad is about DC Villains sent off to fight a battle to the Death");

        assertEquals(Score.ONE, score(item1, item2));
    }

    @Test
    public void testDescriptionMatching3() {
        Item item1 = itemWithTitle("something");
        Item item2 = itemWithTitle("s else");
        item1.setLongDescription("suicide squad Is a film about Several evil villains From DC Comics that get sent off to fight an impossible scenario");
        item2.setLongDescription("The new Comics hit film Suicide Squad is about DC Villains sent off to fight a battle to the Death");

        assertEquals(Score.nullScore(), score(item1, item2));
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

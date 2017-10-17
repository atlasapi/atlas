package org.atlasapi.equiv.scorers;

import static org.junit.Assert.assertEquals;

import org.atlasapi.equiv.results.description.DefaultDescription;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResults;
import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Version;
import org.atlasapi.persistence.testing.StubContentResolver;

import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;

public class TitleSubsetBroadcastItemScorerTest {

    private StubContentResolver resolver;
    private TitleSubsetBroadcastItemScorer scorer;

    @Before
    public void setUp() {
        resolver = new StubContentResolver();
        scorer = new TitleSubsetBroadcastItemScorer(
                resolver, Score.nullScore(), 100);
    }
    
    @Test
    public void testCommonWordsMatches() {
        assertEquals(Score.ONE, score(
                itemWithTitle("The Ren & Stimpy Show"),
                itemWithTitle("Ren and Stimpy!")
        ));
        assertEquals(Score.ONE, score(
                itemWithContainerTitle("The Ren & Stimpy Show"),
                itemWithTitle("Ren and Stimpy!")
        ));
        assertEquals(Score.ONE, score(
                itemWithTitle("New: Uncle"),
                itemWithTitle("Uncle")
        ));
        assertEquals(Score.ONE, score(
                itemWithTitle("New: Uncle"),
                itemWithContainerTitle("Uncle")
        ));
    }

    @Test
    public void testOtherMatches() {
        /* TODO: MBST-18584 this shouldn't match, unfortunately because the scorer checks the
         * percentage of words in shorter title that are in the longer, there's no way of fixing
         * this without a re-design. */
        assertEquals(Score.ONE, score(
                itemWithTitle("Doctor Who?"),
                itemWithTitle("Doctor Who Confidential")
        ));
    }

    @Test
    public void testPunctuationMatches() {
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
    public void testOnlyCommonOrIgnored() {
        assertEquals(Score.ONE, score(
                itemWithTitle("The 100"),
                itemWithTitle("the 100")
        ));
        assertEquals(Score.ONE, score(
                itemWithTitle("The BIG Show"),
                itemWithTitle("the big show")
        ));
        assertEquals(Score.ONE, score(
                itemWithTitle("The 100"),
                itemWithContainerTitle("the 100")
        ));
        assertEquals(Score.ONE, score(
                itemWithContainerTitle("the 100"),
                itemWithTitle("The 100")
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
        Broadcast broadcast = new Broadcast(
                "http://ref.atlasapi.org/channels/pressassociation.com/2021",
                DateTime.now(),
                DateTime.now()
        );
        version.addBroadcast(broadcast);
        item1.addVersion(version);
        assertEquals(Score.nullScore(), score(
                item1,
                item2
        ));
    }

    private Score score(Item subject, Item candidate) {
        DefaultDescription desc = new DefaultDescription();
        EquivToTelescopeResults equivToTelescopeResults =
                EquivToTelescopeResults.create("id", "publisher");

        ScoredCandidates<Item> scores = scorer.score(
                subject,
                ImmutableSet.of(candidate),
                desc,
                equivToTelescopeResults
        );
        return scores.candidates().get(candidate);
    }

    private Item itemWithTitle(String title) {
        Item item = new Item(title, title, Publisher.PA);
        item.setTitle(title);
        return item;
    }

    private Item itemWithContainerTitle(String title) {
        Item item = new Item(title, title, Publisher.PA);
        String uri = "container:" + title;
        Container container = new Container(uri, uri, Publisher.PA);
        container.setTitle(title);
        resolver.respondTo(container);
        item.setContainer(container);
        return item;
    }

}

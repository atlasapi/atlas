package org.atlasapi.equiv.scorers;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.atlasapi.equiv.results.description.DefaultDescription;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.equiv.scorers.aenetworks.AeTitleMatchingItemScorer;
import org.atlasapi.equiv.scorers.barb.BarbTitleMatchingItemScorer;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResult;
import org.atlasapi.media.entity.*;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ResolvedContent;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AeTitleMatchingItemScorerTest {
    private static final Joiner SPACE_JOINER = Joiner.on(' ');

    private static final Score scoreOnMatch = Score.valueOf(2D);
    private static final Score scoreOnPartialMatch = Score.ONE;
    private static final Score scoreOnMismatch = Score.ZERO;
    private static final String AE_NETWORKS_EPISODE_NUMBER_FIELD_NAME = "ae:episode_number";

    private final ContentResolver contentResolver = mock(ContentResolver.class);

    private AeTitleMatchingItemScorer scorer;
    private AeTitleMatchingItemScorer cachedScorer;

    @Before
    public void setUp() throws Exception {
        scorer = AeTitleMatchingItemScorer.builder()
                .withScoreOnPerfectMatch(scoreOnMatch)
                .withScoreOnPartialMatch(scoreOnPartialMatch)
                .withScoreOnMismatch(scoreOnMismatch)
                .withContentResolver(contentResolver)
                .withContainerCacheDuration(0) //caching will break some tests due to reusing the same brand uri
                .build();

        cachedScorer = AeTitleMatchingItemScorer.builder()
                .withScoreOnPerfectMatch(scoreOnMatch)
                .withScoreOnPartialMatch(scoreOnPartialMatch)
                .withScoreOnMismatch(scoreOnMismatch)
                .withContentResolver(contentResolver)
                .withContainerCacheDuration(60)
                .build();
    }

    @Test
    public void testAeNetworksTxlogCustomRuleExamples() {
        assertTxlogAeScore("The Drone Wars 20", "Storage Wars 2 - 2 - Drone Wars 20, The", "23", scoreOnMatch);
        assertTxlogAeScore("aliens", "new: aliens", "23", scoreOnMatch);
        assertTxlogAeScore("weather", "weather episode 7", "23", scoreOnMatch);
        assertTxlogAeScore("weather", "weather series 11", "3", scoreOnMatch);
        assertTxlogAeScore("New: World's Deadliest Weather", "World's Deadliest Weather S1 - - Episode 7", "3", scoreOnPartialMatch);
        assertTxlogAeScore("New: World's Deadliest Weather", "", "3", scoreOnMismatch);
    }

    @Test
    public void testRealAeNetworksTxlogCustomRuleExamples() {
        assertTxlogAeScore("The Shocker", "Hardcore Pawn 7 - 7 - Shocker # 2, The", "24", scoreOnMatch);
        assertTxlogAeScore("Knuckleheads", "American Pickers 4 - 4 - Knuckleheads # 18 (62)","24", scoreOnMatch);
        assertTxlogAeScore("Deadly Hymn", "Deadly Hymn", "24", scoreOnMatch);
        assertTxlogAeScore("Unspeakable - Part Three", "Unspeakable Part 3 <Copy>", "24", scoreOnMatch);
        assertTxlogAeScore("Military v UFOs", "UFO Hunters 1 - 1 - Military Vs. UFOs #11", "24", scoreOnMatch);
        assertTxlogAeScore("Unspeakable - Part One", "Unspeakable Part 1 <Copy>", "24", scoreOnMatch);
        assertTxlogAeScore("There's a New Team in Town - Part Two", "There's A New Team In Town - Part 2 #175B", "24", scoreOnMatch);
        assertTxlogAeScore("Seth v Rich", "Seth Vs. Rich # 11", "24", scoreOnMatch);
        assertTxlogAeScore("Everyone's Replaceable - Even Abby", "Everyone's Replaceable...Even Abby #176", "24", scoreOnMatch);
        assertTxlogAeScore("A New Baby Born And Love All Around! - Part Two", "A New Baby Born and Love All Around Part 2", "24", scoreOnMatch);
        assertTxlogAeScore("The Drone Wars", "Storage Wars 2 - 2 - Drone Wars 24, The", "24", scoreOnMatch);
        assertTxlogAeScore("Senior Centre Showdown", "Storage Wars 1 - 1 - Senior Center Showdown", "24", scoreOnMatch);
    }

    private void assertTxlogAeScore(
            String txlogTitle,
            String aeNetworksTitle,
            String episodeNumber,
            Score expectedScore
    ) {
        Item txlog = txlog(txlogTitle);
        Item aeNetworks = aeItem(aeNetworksTitle, episodeNumber);
        assertThat(score(txlog, aeNetworks), is(expectedScore));
        assertThat(score(aeNetworks, txlog), is(expectedScore));
    }

    private Score score(Item subject, Item candidate) {
        return score(subject, candidate, scorer);
    }

    private Score score(Item subject, Item candidate, AeTitleMatchingItemScorer scorer) {
        ScoredCandidates<Item> scoredCandidates = scorer.score(
                subject,
                ImmutableSet.of(candidate),
                new DefaultDescription(),
                EquivToTelescopeResult.create(subject.getCanonicalUri(), subject.getPublisher().key())
        );
        return scoredCandidates.candidates().get(candidate);
    }

    private Item txlog(String title) {
        Item txlog = new Item();
        txlog.setTitle(title);
        txlog.setPublisher(Publisher.BARB_TRANSMISSIONS);
        return txlog;
    }

    private Item aeItem(String title, String episodeNumber) {
        Item item = new Item();
        item.setTitle(title);
        item.setPublisher(Publisher.AE_NETWORKS);
        Map<String, String> customFields = new HashMap<>();
        customFields.put(AE_NETWORKS_EPISODE_NUMBER_FIELD_NAME, episodeNumber);
        item.setCustomFields(customFields);
        return item;
    }
}
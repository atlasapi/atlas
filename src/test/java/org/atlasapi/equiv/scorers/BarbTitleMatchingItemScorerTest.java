package org.atlasapi.equiv.scorers;

import com.google.common.collect.ImmutableSet;
import org.atlasapi.equiv.results.description.DefaultDescription;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.equiv.scorers.barb.BarbTitleMatchingItemScorer;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResult;
import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.ParentRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ResolvedContent;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BarbTitleMatchingItemScorerTest {
    private static final Score scoreOnMatch = Score.valueOf(2D);
    private static final Score scoreOnPartialMatch = Score.ONE;
    private static final Score scoreOnMismatch = Score.ZERO;

    private final ContentResolver contentResolver = mock(ContentResolver.class);

    private BarbTitleMatchingItemScorer scorer;
    private BarbTitleMatchingItemScorer cachedScorer;

    @Before
    public void setUp() throws Exception {
        scorer = BarbTitleMatchingItemScorer.builder()
                .withScoreOnPerfectMatch(scoreOnMatch)
                .withScoreOnPartialMatch(scoreOnPartialMatch)
                .withScoreOnMismatch(scoreOnMismatch)
                .withContentResolver(contentResolver)
                .withContainerCacheDuration(0) //caching will break some tests due to reusing the same brand uri
                .build();

        cachedScorer = BarbTitleMatchingItemScorer.builder()
                .withScoreOnPerfectMatch(scoreOnMatch)
                .withScoreOnPartialMatch(scoreOnPartialMatch)
                .withScoreOnMismatch(scoreOnMismatch)
                .withContentResolver(contentResolver)
                .withContainerCacheDuration(60)
                .build();
    }

    @Test
    public void testBbcTxlogCustomRuleExamples() {
        assertTxlogNitroScore("BBC SPECIAL TEN O'CLOCK NEWS", "06/06/2019", "BBC Special News at Ten", scoreOnMatch);
        assertTxlogNitroScore("TEN O'CLOCK NEWS", "06/06/2019", "Ten O'Clock News", scoreOnMatch);
    }

    @Test
    public void testRealBbcTxlogCustomRuleExamples() {
        assertTxlogNitroScore("NEWSLINE", "05/06/2019", "BBC Newsline", scoreOnMatch);
        assertTxlogNitroScore("NEWS 24", "07/06/2019", "Joins BBC News", scoreOnMatch);
        assertTxlogNitroScore("TEN O'CLOCK NEWS", "06/06/2019", "BBC News at Ten", scoreOnMatch);
        assertTxlogNitroScore("SIX O'CLOCK NEWS", "06/06/2019", "BBC News at Six", scoreOnMatch);
        assertTxlogNitroScore("BBC News at One", "06/06/2019", "BBC News at One", scoreOnMatch);
        assertTxlogNitroScore("ONE O'CLOCK NEWS", "06/06/2019", "BBC News at One", scoreOnMatch);
        assertTxlogNitroScore("WALES TODAY", "09/06/2019", "BBC Wales Today", scoreOnMatch);
        assertTxlogNitroScore("!MPOSSIBLE", "Episode 7", "Impossible", scoreOnMatch);
        assertTxlogNitroScore("BBC NEWS", "08/06/2019", "BBC Weekend News", scoreOnMatch);
        assertTxlogNitroScore("INTERNATIONAL FOOTBALL HIGHLIGHTS", "Estonia v Northern Ireland", "International Football", scoreOnMatch);
    }

    @Test
    public void testRealBbcTxlogRegularExamples() {
        assertTxlogNitroScore("THE HOUSING ENFORCERS", "Episode 2", "The Housing Enforcers", scoreOnMatch);
    }

    private void assertTxlogNitroScore(
            String txlogTitle,
            String nitroEpisodeTitle,
            String nitroBrandTitle,
            Score expectedScore
    ) {
        Item txlog = txlog(txlogTitle);
        Brand nitroBrand = nitroBrand(nitroBrandTitle, "1");
        Episode nitroEpisode = nitroEpisode(nitroEpisodeTitle, nitroBrand);
        setUpContentResolving(nitroBrand);
        assertThat(score(txlog, nitroEpisode), is(expectedScore));
        assertThat(score(nitroEpisode, txlog), is(expectedScore));
    }

    private Score score(Item subject, Item candidate) {
        ScoredCandidates<Item> scoredCandidates = scorer.score(
                subject,
                ImmutableSet.of(candidate),
                new DefaultDescription(),
                EquivToTelescopeResult.create(subject.getCanonicalUri(), subject.getPublisher().key())
        );
        return scoredCandidates.candidates().get(candidate);
    }

    private Score cachedScore(Item subject, Item candidate) {
        ScoredCandidates<Item> scoredCandidates = cachedScorer.score(
                subject,
                ImmutableSet.of(candidate),
                new DefaultDescription(),
                EquivToTelescopeResult.create(subject.getCanonicalUri(), subject.getPublisher().key())
        );
        return scoredCandidates.candidates().get(candidate);
    }

    private void setUpContentResolving(Content content) {
        when(contentResolver.findByCanonicalUris(
                ImmutableSet.of(content.getCanonicalUri()))
        ).thenReturn(
                ResolvedContent.builder()
                        .put(content.getCanonicalUri(), content)
                        .build()
        );
    }

    private Item txlog(String title) {
        Item txlog = new Item();
        txlog.setTitle(title);
        txlog.setPublisher(Publisher.BARB_TRANSMISSIONS);
        return txlog;
    }

    private Brand nitroBrand(String title, String uri) {
        Brand nitroBrand = new Brand();
        nitroBrand.setTitle(title);
        nitroBrand.setPublisher(Publisher.BBC_NITRO);
        nitroBrand.setCanonicalUri(uri);
        return nitroBrand;
    }

    private Episode nitroEpisode(String title, Brand nitroBrand) {
        Episode nitroEpisode = new Episode();
        nitroEpisode.setTitle(title);
        nitroEpisode.setPublisher(Publisher.BBC_NITRO);
        nitroEpisode.setParentRef(ParentRef.parentRefFrom(nitroBrand));
        return nitroEpisode;
    }

    @Test
    public void testBrandsAreCached() {
        Item txlog = txlog("t1");
        Brand nitroBrand = nitroBrand("t1", "1");
        Episode nitroEpisode = nitroEpisode("t2", nitroBrand);
        setUpContentResolving(nitroBrand);
        assertThat(score(txlog, nitroEpisode), is(scoreOnMatch));
        assertThat(score(nitroEpisode, txlog), is(scoreOnMatch));
        assertThat(cachedScore(txlog, nitroEpisode), is(scoreOnMatch));
        assertThat(cachedScore(nitroEpisode, txlog), is(scoreOnMatch));
        nitroBrand = nitroBrand("t3", "1");
        setUpContentResolving(nitroBrand);
        assertThat(score(txlog, nitroEpisode), is(scoreOnMismatch));
        assertThat(score(nitroEpisode, txlog), is(scoreOnMismatch));
        assertThat(cachedScore(txlog, nitroEpisode), is(scoreOnMatch));
        assertThat(cachedScore(nitroEpisode, txlog), is(scoreOnMatch));
    }

}
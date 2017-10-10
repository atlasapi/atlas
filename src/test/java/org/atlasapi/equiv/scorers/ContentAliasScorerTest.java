package org.atlasapi.equiv.scorers;

import com.google.common.collect.ImmutableSet;
import org.atlasapi.equiv.results.description.DefaultDescription;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class ContentAliasScorerTest {
    private final Score mismatchScore = Score.nullScore();

    private final ContentAliasScorer scorer = new ContentAliasScorer(mismatchScore);

    @Test
    public void testScoresMismatchIfNoContentAliasUrlMatch(){

        Item subject = new Item("subj", "subj", Publisher.YOUVIEW);
        subject.setAliasUrls(ImmutableSet.of("Something"));

        Item candidate = new Item("cand", "cand", Publisher.PA);
        candidate.setAliasUrls(ImmutableSet.of("Anything"));

        ScoredCandidates<Item> results = scorer.score(
                subject,
                ImmutableSet.of(candidate),
                new DefaultDescription());

        assertThat(results.candidates().get(candidate), is(mismatchScore));
    }

    @Test
    public void testScoresOneIfContentAliasUrlsMatch(){

        Item subject = new Item("subj", "subj", Publisher.YOUVIEW);
        subject.setAliasUrls(ImmutableSet.of("Something"));

        Item candidate = new Item("cand", "cand", Publisher.PA);
        candidate.setAliasUrls(ImmutableSet.of("Something"));

        ScoredCandidates<Item> results = scorer.score(
                subject,
                ImmutableSet.of(candidate),
                new DefaultDescription());

        assertThat(results.candidates().get(candidate), is(Score.ONE));
    }

    @Test
    public void testScoresMismatchIfNoContentAliasMatch(){

        Item subject = new Item("subj", "subj", Publisher.YOUVIEW);
        Alias subjAlias = new Alias("subjAlias one", "subjAlias two");
        subject.setAliases(ImmutableSet.of(subjAlias));

        Item candidate = new Item("cand", "cand", Publisher.PA);
        Alias candAlias = new Alias("candAlias one", "candAlias two");
        candidate.setAliases(ImmutableSet.of(candAlias));

        ScoredCandidates<Item> results = scorer.score(
                subject,
                ImmutableSet.of(candidate),
                new DefaultDescription());

        assertThat(results.candidates().get(candidate), is(mismatchScore));
    }

    @Test
    public void testScoresOneIfContentAliasMatch(){

        Alias alias = new Alias("alias one", "alias two");

        Item subject = new Item("subj", "subj", Publisher.YOUVIEW);
        subject.setAliases(ImmutableSet.of(alias));

        Item candidate = new Item("cand", "cand", Publisher.PA);
        candidate.setAliases(ImmutableSet.of(alias));

        ScoredCandidates<Item> results = scorer.score(
                subject,
                ImmutableSet.of(candidate),
                new DefaultDescription());

        assertThat(results.candidates().get(candidate), is(Score.ONE));
    }
}
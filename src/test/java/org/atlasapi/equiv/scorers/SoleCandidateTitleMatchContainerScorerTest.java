package org.atlasapi.equiv.scorers;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.atlasapi.equiv.results.description.DefaultDescription;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResult;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.SearchResolver;
import org.atlasapi.search.model.SearchQuery;

import com.metabroadcast.applications.client.model.internal.Application;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;

import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SoleCandidateTitleMatchContainerScorerTest {

    private final SearchResolver resolver = mock(SearchResolver.class);
    private final SoleCandidateTitleMatchContainerScorer scorer =
            new SoleCandidateTitleMatchContainerScorer(Score.valueOf(2.0), Score.nullScore(), resolver);

    private AtomicInteger counter;

    @Before
    public void setUp(){

        counter = new AtomicInteger();
        Iterable<Container> candidates = null;
        when(resolver.search(Matchers.any(SearchQuery.class), Matchers.any(Application.class))).thenReturn(null);
    }

    @Test
    public void testScoresCandidatesWithSameTitle(){
//        DefaultDescription desc = new DefaultDescription();
        EquivToTelescopeResult equivToTelescopeResult = EquivToTelescopeResult.create(
                "id",
                "publisher"
        );

        Container subject = containerWithTitle("Doctor Who", Publisher.PA);
        Container candidate1 = containerWithTitle("Doctor Who", Publisher.IMDB);
        Container candidate2 = containerWithTitle("Doctor Who", Publisher.BBC_NITRO);
        Container candidate3 = containerWithTitle("Doctor Who Christmas Specials", Publisher.BBC_NITRO);

        List<Identified> containers = ImmutableList.of();
        when(resolver.search(Matchers.any(SearchQuery.class), Matchers.any(Application.class))).thenReturn(containers);
        Set<Container> candidates = ImmutableSet.of(candidate1, candidate2, candidate3);

        ScoredCandidates<Container> results = scorer.score(
                subject,
                candidates,
                new DefaultDescription(),
                equivToTelescopeResult
        );

        assertThat(results.candidates().get(candidate1), is(Score.valueOf(2.0)));
        assertThat(results.candidates().get(candidate2), is(Score.valueOf(2.0)));
        assertThat(results.candidates().get(candidate3), is(Score.nullScore()));
    }

    @Test
    public void testScoresOnlyIfSoleCandidate(){
        //        DefaultDescription desc = new DefaultDescription();
        EquivToTelescopeResult equivToTelescopeResult = EquivToTelescopeResult.create(
                "id",
                "publisher"
        );

        Container subject = containerWithTitle("Doctor Who", Publisher.PA);
        Container sameTitleSamePub1 = containerWithTitle("Doctor Who", Publisher.BBC_NITRO);
        Container sameTitleSamePub2 = containerWithTitle("Doctor Who", Publisher.BBC_NITRO);
        Container diffTitleSamePub = containerWithTitle("Doctor Who Christmas Specials", Publisher.BBC_NITRO);

        List<Identified> containers = ImmutableList.of();
        when(resolver.search(Matchers.any(SearchQuery.class), Matchers.any(Application.class))).thenReturn(containers);
        Set<Container> candidates = ImmutableSet.of(sameTitleSamePub1, sameTitleSamePub2, diffTitleSamePub);

        ScoredCandidates<Container> results = scorer.score(
                subject,
                candidates,
                new DefaultDescription(),
                equivToTelescopeResult
        );

        assertThat(results.candidates().get(sameTitleSamePub1), is(Score.nullScore()));
        assertThat(results.candidates().get(sameTitleSamePub2), is(Score.nullScore()));
        assertThat(results.candidates().get(diffTitleSamePub), is(Score.nullScore()));
    }

    @Test
    public void testNoScoringIfContentWithSameTitleAndPublisherFound(){
        //        DefaultDescription desc = new DefaultDescription();
        EquivToTelescopeResult equivToTelescopeResult = EquivToTelescopeResult.create(
                "id",
                "publisher"
        );

        Container subject = containerWithTitle("Doctor Who", Publisher.PA);
        Container sameTitleSubjectPubFound = containerWithTitle("Doctor Who", Publisher.PA);

        Container sameTitleSamePub = containerWithTitle("Doctor Who", Publisher.BBC_NITRO);
        Container diffTitleSamePub = containerWithTitle("Doctor Who Christmas Specials", Publisher.BBC_NITRO);

        List<Identified> containers = ImmutableList.of(sameTitleSubjectPubFound);
        when(resolver.search(Matchers.any(SearchQuery.class), Matchers.any(Application.class))).thenReturn(containers);
        Set<Container> candidates = ImmutableSet.of(sameTitleSamePub, diffTitleSamePub);

        ScoredCandidates<Container> results = scorer.score(
                subject,
                candidates,
                new DefaultDescription(),
                equivToTelescopeResult
        );

        assertThat(results.candidates().get(sameTitleSamePub), is(Score.nullScore()));
        assertThat(results.candidates().get(diffTitleSamePub), is(Score.nullScore()));
    }

    @Test
    public void testScoresIfContentWithSameTitleButDiffPublisherFound(){
        //        DefaultDescription desc = new DefaultDescription();
        EquivToTelescopeResult equivToTelescopeResult = EquivToTelescopeResult.create(
                "id",
                "publisher"
        );

        Container subject = containerWithTitle("Doctor Who", Publisher.PA);
        Container sameTitleSubjectPubFound = containerWithTitle("Doctor Who Christmas Specials", Publisher.PA);

        Container sameTitleSamePub = containerWithTitle("Doctor Who", Publisher.BBC_NITRO);
        Container diffTitleSamePub = containerWithTitle("Doctor Who Christmas Specials", Publisher.BBC_NITRO);

        List<Identified> containers = ImmutableList.of(sameTitleSubjectPubFound);
        when(resolver.search(Matchers.any(SearchQuery.class), Matchers.any(Application.class))).thenReturn(containers);
        Set<Container> candidates = ImmutableSet.of(sameTitleSamePub, diffTitleSamePub);

        ScoredCandidates<Container> results = scorer.score(
                subject,
                candidates,
                new DefaultDescription(),
                equivToTelescopeResult
        );

        assertThat(results.candidates().get(sameTitleSamePub), is(Score.valueOf(2.0)));
        assertThat(results.candidates().get(diffTitleSamePub), is(Score.nullScore()));
    }

    @Test
    public void testSkipsAlreadyScoredPubs(){
        //        DefaultDescription desc = new DefaultDescription();
        EquivToTelescopeResult equivToTelescopeResult = EquivToTelescopeResult.create(
                "id",
                "publisher"
        );

        Container subject = containerWithTitle("Doctor Who", Publisher.PA);
        Container sameTitleSamePub1 = containerWithTitle("Doctor Who", Publisher.BBC_NITRO);
        Container diffTitleSamePub = containerWithTitle("Doctor Who Christmas Specials", Publisher.BBC_NITRO);

        List<Identified> containers = ImmutableList.of();
        when(resolver.search(Matchers.any(SearchQuery.class), Matchers.any(Application.class))).thenReturn(containers);
        Set<Container> candidates = ImmutableSet.of(sameTitleSamePub1, diffTitleSamePub);

        ScoredCandidates<Container> results = scorer.score(
                subject,
                candidates,
                new DefaultDescription(),
                equivToTelescopeResult
        );

        assertThat(results.candidates().get(sameTitleSamePub1), is(Score.valueOf(2.0)));
        assertThat(results.candidates().get(diffTitleSamePub), is(Score.nullScore()));
    }

    private void score(double expected, ScoredCandidates<Container> scores) {
        Score value = Iterables.getOnlyElement(scores.candidates().entrySet()).getValue();
        assertTrue(String.format("expected %s got %s", expected, value), value.equals(expected > 0 ? Score.valueOf(expected) : Score.NULL_SCORE));
    }

    private Container containerWithTitle(String title, Publisher publisher) {
        long id = counter.incrementAndGet();
        Container container = new Container(
                "uri."+title+publisher.title() + id,
                "curie",
                publisher
        );
        container.setId(id);
        container.setTitle(title);
        container.setYear(2013);
        return container;
    }

}

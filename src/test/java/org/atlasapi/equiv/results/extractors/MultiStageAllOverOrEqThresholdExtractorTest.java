package org.atlasapi.equiv.results.extractors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.atlasapi.equiv.results.description.DefaultDescription;
import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidate;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResults;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.junit.Test;

import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class MultiStageAllOverOrEqThresholdExtractorTest {
    private final MultiStageAllOverOrEqThresholdExtractor<Content> extractor =
            new MultiStageAllOverOrEqThresholdExtractor<>(
                    ImmutableSet.of(5D, 3D, 1D)
            );

    private final ResultDescription desc = new DefaultDescription();
    private final EquivToTelescopeResults equivToTelescopeResults =
            EquivToTelescopeResults.create("id", "publisher");


    @Test
    public void testExtractFrom3ToBelow5() {
        Set<ScoredCandidate<Content>> expected =
                ImmutableSet.of(
                        candidate(1, Score.valueOf(3.0)),
                        candidate(2, Score.valueOf(4.9)),
                        candidate(3, Score.valueOf(4.0))
                );
        Set<ScoredCandidate<Content>> notExpected =
                ImmutableSet.of(
                        candidate(4, Score.valueOf(1.0)),
                        candidate(5, Score.valueOf(0.0))
                );
        Set<ScoredCandidate<Content>> scoredCandidates = extractor.extract(
                ImmutableList.<ScoredCandidate<Content>>builder().addAll(expected).addAll(notExpected).build(),
                new Item(),
                desc,
                equivToTelescopeResults
        );
        assertThat(scoredCandidates, is(expected));
    }

    @Test
    public void testExtractAllOverOrEq5() {
        Set<ScoredCandidate<Content>> expected =
                ImmutableSet.of(
                        candidate(1, Score.valueOf(5.0)),
                        candidate(2, Score.valueOf(10.0))
                );
        Set<ScoredCandidate<Content>> notExpected =
                ImmutableSet.of(
                        candidate(3, Score.valueOf(4.9)),
                        candidate(4, Score.valueOf(1.0)),
                        candidate(5, Score.valueOf(0.0))
                );
        Set<ScoredCandidate<Content>> scoredCandidates = extractor.extract(
                ImmutableList.<ScoredCandidate<Content>>builder().addAll(expected).addAll(notExpected).build(),
                new Item(),
                desc,
                equivToTelescopeResults
        );
        assertThat(scoredCandidates, is(expected));
    }

    @Test
    public void testExcludesScoresBelow1() {
        Set<ScoredCandidate<Content>> expected =
                ImmutableSet.of();
        Set<ScoredCandidate<Content>> notExpected =
                ImmutableSet.of(
                        candidate(1, Score.valueOf(0.9)),
                        candidate(2, Score.valueOf(0.0))
                );
        Set<ScoredCandidate<Content>> scoredCandidates = extractor.extract(
                ImmutableList.<ScoredCandidate<Content>>builder().addAll(expected).addAll(notExpected).build(),
                new Item(),
                desc,
                equivToTelescopeResults
        );
        assertThat(scoredCandidates, is(expected));
    }

    @Test
    public void testExcludeNullScores() {
        Set<ScoredCandidate<Content>> expected =
                ImmutableSet.of(
                        candidate(1, Score.valueOf(1.0)),
                        candidate(2, Score.valueOf(2.0))
                );
        Set<ScoredCandidate<Content>> notExpected =
                ImmutableSet.of(
                        candidate(3, Score.nullScore()),
                        candidate(4, Score.nullScore())
                );
        Set<ScoredCandidate<Content>> scoredCandidates = extractor.extract(
                ImmutableList.<ScoredCandidate<Content>>builder().addAll(expected).addAll(notExpected).build(),
                new Item(),
                desc,
                equivToTelescopeResults
        );
        assertThat(scoredCandidates, is(expected));
    }


    private ScoredCandidate<Content> candidate(int id, Score score) {
        return ScoredCandidate.valueOf(new Item("test" + id,"cur" + id, Publisher.BBC), score);
    }
}
package org.atlasapi.equiv.generators;

import org.atlasapi.equiv.results.description.DefaultDescription;
import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeComponent;
import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Publisher;

import com.google.common.base.Functions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ContentTitleScorerTest {

    private static final Score EXACT_MATCH_SCORE = Score.valueOf(2.0);
    private final ContentTitleScorer<Container> scorer =
            new ContentTitleScorer<>(
                    "Title",
                    Functions.identity(),
                    EXACT_MATCH_SCORE,
                    Score.ONE
            );

    private final ResultDescription desc = new DefaultDescription();

    private ScoredCandidates<Container> score(String subject, String candidate) {
        return scorer.scoreCandidates(
                brandWithTitle(subject),
                ImmutableList.of(brandWithTitle(candidate)),
                desc,
                EquivToTelescopeComponent.create()
        );
    }
    
    @Test
    public void testScore() {
        scoreLt(0.01, score("biker", "nothing"));
        score(EXACT_MATCH_SCORE, score("biker", "biker"));
        scoreLt(0.1, score("biker", "bike"));
        scoreLt(0.1, score("biker", "bikers"));
    }

    @Test
    public void testScoreSymmetry() {
        assertEquals(score("biker", "biker"), score("biker", "biker"));
        assertEquals(score("bike", "biker"), score("biker", "bike"));
        assertEquals(score("bikers", "biker"), score("biker", "bikers"));
    }
    
    @Test
    public void testScoreWithAmpersands() {
        score(EXACT_MATCH_SCORE, score("Rosencrantz & Guildenstern Are Dead", "Rosencrantz and Guildenstern Are Dead"));
        score(EXACT_MATCH_SCORE, score("Bill & Ben", "Bill and Ben"));
    }
    
    @Test
    public void testScoreWithCommonPrefix() {
        score(EXACT_MATCH_SCORE, score("The Great Escape", "The Great Escape"));
        scoreLt(0.1, score("The Great Escape", "Italian Job"));
        
        score(EXACT_MATCH_SCORE, score("The Great Escape", "Great Escape"));
        score(EXACT_MATCH_SCORE, score("the Great Escape", "Great Escape"));
        scoreLt(0.1, score("Theatreland", "The atreland"));
        scoreLt(0.1, score("theatreland", "the atreland"));
    }
    
    private void scoreLt(double expected, ScoredCandidates<Container> candidates) {
        Score score = scoredOfOnly(candidates);
        assertThat(score.asDouble(), is(lessThan(expected)));
    }

    private Score scoredOfOnly(ScoredCandidates<Container> candidates) {
        return Iterables.getOnlyElement(candidates.candidates().entrySet()).getValue();
    }

    private void score(Score expected, ScoredCandidates<Container> candidates) {
        Score score = scoredOfOnly(candidates);
        assertTrue(String.format("expected %s got %s", expected, score), score.equals(expected.asDouble() > 0 ? expected : Score.NULL_SCORE));
    }

    private Container brandWithTitle(String title) {
        Brand brand = new Brand("uri","curie",Publisher.BBC);
        brand.setTitle(title);
        return brand;
    }


}

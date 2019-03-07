package org.atlasapi.equiv.generators;

import junit.framework.TestCase;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.is;

public class FilmTitleMatcherTest extends TestCase {

    private final FilmTitleMatcher matcher = new FilmTitleMatcher(new ExpandingTitleTransformer());

    @Test
    public void testPerfectMatch() {
        assertThat(matcher.match("equal", "equal").asDouble(), is(1.0));
    }

    @Test
    public void testDifferentMatch() {
        assertThat(matcher.match("equal", "different").asDouble(), is(0.0));
    }

    @Test
    public void testShortDifferentMatch() {
        assertThat(matcher.match("e", "d").asDouble(), is(0.0));
    }

    @Test
    public void testOneLetterMore() {
        assertThat(matcher.match("downweighthis", "downweighthiss").asDouble(), is(closeTo(0.1, 0.0000001)));
    }

    @Test
    public void testShortOneLetterMore() {
        assertThat(matcher.match("ds", "d").asDouble(), is(closeTo(0.1, 0.0000001)));
    }

    public void testSymmetry() {
        assertEquals(matcher.match("downweighthiss", "downweighthis"),matcher.match("downweighthis", "downweighthiss"));
    }

    public void testDoubleLength() {
        assertThat(matcher.match("equals", "equalsequals").asDouble(),is(closeTo(0.0, 0.0000001)));
    }

    public void testDoubleLengthSymmetry() {
        assertThat(matcher.match("equalsequals", "equals").asDouble(),is(closeTo(0.0, 0.0000001)));
    }

    public void testMoreThanDoubleIsntNegative() {
        assertThat(matcher.match("equal", "equalequals").asDouble(),is(closeTo(0.0, 0.0000001)));
    }
    
}

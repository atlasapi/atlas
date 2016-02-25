package org.atlasapi.equiv.generators;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.is;

import org.junit.Test;

import junit.framework.TestCase;

public class FilmTitleMatcherTest extends TestCase {

    private final FilmTitleMatcher matcher = new FilmTitleMatcher(new ExpandingTitleTransformer());

    @Test
    public void testPerfectMatch() {
        assertThat(matcher.match("equal", "equal"), is(1.0));
    }

    @Test
    public void testDifferentMatch() {
        assertThat(matcher.match("equal", "different"), is(0.0));
    }

    @Test
    public void testShortDifferentMatch() {
        assertThat(matcher.match("e", "d"), is(0.0));
    }

    @Test
    public void testOneLetterMore() {
        assertThat(matcher.match("downweighthis", "downweighthiss"), is(closeTo(0.1, 0.0000001)));
    }

    @Test
    public void testShortOneLetterMore() {
        assertThat(matcher.match("ds", "d"), is(closeTo(0.1, 0.0000001)));
    }

    public void testSymmetry() {
        assertEquals(matcher.match("downweighthiss", "downweighthis"),matcher.match("downweighthis", "downweighthiss"));
    }

    public void testDoubleLength() {
        assertThat(matcher.match("equals", "equalsequals"),is(closeTo(0.0, 0.0000001)));
    }

    public void testDoubleLengthSymmetry() {
        assertThat(matcher.match("equalsequals", "equals"),is(closeTo(0.0, 0.0000001)));
    }

    public void testMoreThanDoubleIsntNegative() {
        assertThat(matcher.match("equal", "equalequals"),is(closeTo(0.0, 0.0000001)));
    }
    
}

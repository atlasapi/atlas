package org.atlasapi.equiv.generators;

import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class TitleAbbreviationTransformTest {

    private final TitleAbbreviationTransform sanitizer = new TitleAbbreviationTransform();

    @Test
    public void testWithoutAbbrevations() {
        assertThat(sanitizer.apply("Sultans of Swing"),
                is("sultans of swing"));
        assertThat(sanitizer.apply("Undefeatablatoration"),
                is("undefeatablatoration"));
        assertThat(sanitizer.apply("Doctor Drake Ramoray"),
                is("doctor drake ramoray"));
    }

    @Test
    public void testExpandsAbbrevation() {
        assertThat(sanitizer.apply("Dr Blake Mysteries"),
                is("doctor blake mysteries"));
        assertThat(sanitizer.apply("Dr Who"),
                is("doctor who"));
    }

}
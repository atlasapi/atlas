package org.atlasapi.equiv.generators;

import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class TitleExpanderTest {

    private final TitleExpander sanitizer = new TitleExpander();

    @Test
    public void testWithoutAbbrevations() {
        assertThat(sanitizer.expand("Sultans of Swing"),
                is("sultans of swing"));
        assertThat(sanitizer.expand("Undefeatablatoration"),
                is("undefeatablatoration"));
        assertThat(sanitizer.expand("Doctor Drake Ramoray"),
                is("doctor drake ramoray"));
    }

    @Test
    public void testExpandsAbbrevation() {
        assertThat(sanitizer.expand("Dr Blake Mysteries"),
                is("doctor blake mysteries"));
        assertThat(sanitizer.expand("Dr Who"),
                is("doctor who"));
    }

}
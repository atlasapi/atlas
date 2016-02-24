package org.atlasapi.equiv.generators;

import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class ExpandingTitleTransformerTest {

    private final ExpandingTitleTransformer sanitizer = new ExpandingTitleTransformer();

    @Test
    public void testDoesntExpandWithoutAbbrevations() {
        assertThat(sanitizer.expand("Sultans of Swing"),
                is("sultans of swing"));
        assertThat(sanitizer.expand("Undefeatablatoration"),
                is("undefeatablatoration"));
        assertThat(sanitizer.expand("Doctor Drake Ramoray"),
                is("doctor drake ramoray"));
        assertThat(sanitizer.expand("3issues"),
                is("3issues"));
    }

    @Test
    public void testExpandsAbbrevation() {
        assertThat(sanitizer.expand("Dr Blake Mysteries"),
                is("doctor blake mysteries"));
        assertThat(sanitizer.expand("Dr Who"),
                is("doctor who"));
        assertThat(sanitizer.expand("Iron man 3"),
                is("iron man three"));
        assertThat(sanitizer.expand("3 musketeers"),
                is("three musketeers"));
    }

}
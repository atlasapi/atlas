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

    @Test
    public void combined(){
        assertThat(sanitizer.expand(""),
                is("five for vendetta"));
        assertThat(sanitizer.expand("Iron man III"),
                is("iron man three"));
        assertThat(sanitizer.expand("XXX"), //Vin Diesel film name
                is("xxx")); //because we only translate up to 20.
    }

    @Test
    public void testRomans(){
        assertThat(sanitizer.expand("V for Vendetta"),
                is("five for vendetta"));
        assertThat(sanitizer.expand("Iron man III"),
                is("iron man three"));
        assertThat(sanitizer.expand("XXX"), //Vin Diesel film name
                is("xxx")); //because we only translate up to 20.
    }

    @Test
    public void testPossesive(){
        assertThat(sanitizer.expand("Noel Fielding's Luxury Comedy"),
                is("noel fielding luxury comedy"));
        assertThat(sanitizer.expand("'s"), //replace standalone 's
                is(""));
        assertThat(sanitizer.expand("ss'ss"), //dont replace the middle of sentences
                is("ss'ss"));
    }

    @Test
    public void testBritisise(){
        assertThat(sanitizer.expand("Harbour of love"), //replace end of word
                is("harbor of love"));
        assertThat(sanitizer.expand("Our love"), //replace whole word
                is("or love"));
        assertThat(sanitizer.expand("Ourselves and us"), //dont replace start of word
                is("ourselves and us"));
    }

}
package org.atlasapi.remotesite.btvod;


import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class TitleSanitiserTest {


    private TitleSanitiser titleSanitiser = new TitleSanitiser();

    @Test
    public void testRemovesZQXFromTitles(){
        String titleWithGarbage1 = "ZQWModern_Family: S01 S1-E4 ZQWThe Incident";
        String titleWithGarbage2 = "ZQZPeppa_Pig: S01 S1-E4 ZQZSchool Play";

        assertThat(titleSanitiser.sanitiseTitle(titleWithGarbage1), is("Modern_Family: S01 S1-E4 The Incident"));
        assertThat(titleSanitiser.sanitiseTitle(titleWithGarbage2), is("Peppa_Pig: S01 S1-E4 School Play"));

    }


    @Test
    public void testDoesntMessUpCorrectTitles(){
        String title = "Modern Family: S03 - HD S3-E17 Truth Be Told - HD";

        assertThat(titleSanitiser.sanitiseTitle(title), is(title));

    }
}
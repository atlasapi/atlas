package org.atlasapi.remotesite.btvod;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;

public class TitleSanitiserTest {

    private TitleSanitiser titleSanitiser = new TitleSanitiser();

    @Test
    public void testRemovesZQXFromTitles(){
        String titleWithGarbage1 = "ZQWModern_Family: S01 S1-E4 ZQWThe Incident";
        String titleWithGarbage2 = "ZQZPeppa_Pig: S01 S1-E4 ZQZSchool Play";
        String titleWithGarbage3 = "ZQWAmerican_Horror_Story: S01 S1-E11 ZQWBirth";

        assertThat(titleSanitiser.sanitiseTitle(titleWithGarbage1), is("The Incident"));
        assertThat(titleSanitiser.sanitiseTitle(titleWithGarbage2), is("School Play"));
        assertThat(titleSanitiser.sanitiseTitle(titleWithGarbage3), is("Birth"));

    }

    @Test
    public void testRemovesCurzonSuffixFromTitles() {
        String titleWithCurzonSuffix = "Film (Curzon)";
        assertThat(titleSanitiser.sanitiseTitle(titleWithCurzonSuffix), is("Film"));
    }

    @Test
    public void testRemovesCurzonSuffixFromHDTitles() {
        String titleWithCurzonSuffix = "Film (Curzon) - HD";
        assertThat(titleSanitiser.sanitiseTitle(titleWithCurzonSuffix), is("Film"));
    }

    @Test
    public void testRemovesHdFromWithinTitle(){
        String title = "Modern Family: S03 S3-E17 Truth Be Told";

        assertThat(titleSanitiser.sanitiseTitle(title), is("Truth Be Told"));
    }
    
    @Test
    public void testRemovesComingSoonSuffix() {
        String titleWithHdSuffix = "Film : Coming Soon";
        assertThat(titleSanitiser.sanitiseTitle(titleWithHdSuffix), is("Film"));
    }
    
    @Test
    public void testRemovesBeforeSuffix() {
        String titleWithHdSuffix = "Film (Before DVD)";
        assertThat(titleSanitiser.sanitiseTitle(titleWithHdSuffix), is("Film"));
    }
    
    @Test
    public void testRemovesHdSuffix() {
        String titleWithHdSuffix = "Film - HD";
        assertThat(titleSanitiser.sanitiseTitle(titleWithHdSuffix), is("Film"));
    }

    @Test
    public void testExtractsEpisodeTitles() {
        assertThat(titleSanitiser.sanitiseTitle("Series Title: S1 S1-E9 Real Title"),
                is("Real Title"));
        assertThat(titleSanitiser.sanitiseTitle("Cashmere Mafia S1-E2 Conference Call"),
                is("Conference Call"));
        assertThat(titleSanitiser.sanitiseTitle(
                        "Classic Premiership Rugby - Saracens v Leicester Tigers 2010/11"),
                is("Saracens v Leicester Tigers 2010/11"));
        assertThat(titleSanitiser.sanitiseTitle("FIFA Films - 1958 Sweden - Hinein! - HD"),
                is("1958 Sweden - Hinein!"));
        assertThat(titleSanitiser.sanitiseTitle("FIFA Films - 1958 Sweden - Hinein!"),
                is("1958 Sweden - Hinein!"));
        assertThat(titleSanitiser.sanitiseTitle(
                        "UFC: The Ultimate Fighter Season 19 - Season 19 Episode 2"),
                is("Episode 2"));
        assertThat(titleSanitiser.sanitiseTitle(
                        "Modern Family: S03 - HD S3-E17 Truth Be Told - HD"),
                is("Truth Be Told"));
        assertThat(titleSanitiser.sanitiseTitle("ZQWModern_Family: S01 S1-E4 ZQWThe_Incident"),
                is("The Incident"));
        assertThat(titleSanitiser.sanitiseTitle("ZQZPeppa_Pig: S01 S1-E4 ZQZSchool Play"),
                is("School Play"));
        assertThat(titleSanitiser.sanitiseTitle(
                        "ZQWAmerican_Horror_Story: S01 S1-E11 ZQWBirth"),
                is("Birth"));
        assertThat(titleSanitiser.sanitiseTitle("Peppa's Circus - HD - Peppa's Circus - HD"),
                is("Peppa's Circus"));
        assertThat(titleSanitiser.sanitiseTitle("ZQWModern_Family: S01 S1 E4 ZQWThe_Incident"),
                is("The Incident"));
    }

    @Test
    public void testStripFilmCollection() throws Exception {
        assertThat(titleSanitiser.sanitiseTitle(
                "Jurassic Park Collection - The Lost World: Jurassic Park"),
                is("The Lost World: Jurassic Park"));
    }
}
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
        
        assertThat(titleSanitiser.sanitiseTitle("Mad Men S01 E01"),
                is(""));
        
        assertThat(titleSanitiser.sanitiseTitle("Mad Men S01 E01 A title"),
                is("A title"));
        
        assertThat(titleSanitiser.sanitiseTitle("Series: S01 S01 E01"),
                is(""));
        
        assertThat(titleSanitiser.sanitiseTitle("Series: S1 S1 E1 Episode 1"),
                is("Episode 1"));
        
        assertThat(titleSanitiser.sanitiseTitle("Ben & Holly's Little Kingdom - Back to Backs - Back to Back 2 - Ben & Holly's Little Kingdom"),
                is("Ben & Holly's Little Kingdom"));
        
        assertThat(titleSanitiser.sanitiseTitle("Ben & Holly's Little Kingdom - Back to Backs - My Collection - Back to Back - Ben & Holly's Little Kingdom"),
                is("Ben & Holly's Little Kingdom"));
        
        assertThat(titleSanitiser.sanitiseTitle("Peppa Pig - Volume 4 - Pedro's Cough / The Library"),
                is("Pedro's Cough / The Library"));
        
        assertThat(titleSanitiser.sanitiseTitle("Peppa Pig - Vol 4 - Pedro's Cough / The Library"),
                is("Pedro's Cough / The Library"));
        
        assertThat(titleSanitiser.sanitiseTitle("Peppa Pig - Vol. 4 - Pedro's Cough / The Library"),
                is("Pedro's Cough / The Library"));
        
    }

    @Test
    public void testStripFilmCollection() throws Exception {
        assertThat(titleSanitiser.sanitiseTitle(
                "Jurassic Park Collection - The Lost World: Jurassic Park"),
                is("The Lost World: Jurassic Park"));
    }
}
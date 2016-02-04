package org.atlasapi.remotesite.pa;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

public class PaTagMapTest {

    private PaTagMap paTagMap;

    @Before
    public void setUp() {
        paTagMap = new PaTagMap();
    }

    @Test
    public void testTagMappingForFilms() {
        Set<String> filmGenres = new HashSet<>(
                Arrays.asList(
                        "http://pressassociation.com/genres/BF01",
                        "http://pressassociation.com/genres/1F18",
                        "http://pressassociation.com/genres/1F06",
                        "http://pressassociation.com/genres/1F03",
                        "http://pressassociation.com/genres/1F1A",
                        "http://pressassociation.com/genres/1600",
                        "http://pressassociation.com/genres/1300",
                        "http://pressassociation.com/genres/1400",
                        "http://pressassociation.com/genres/1000",
                        "http://pressassociation.com/genres/1F19",
                        "http://pressassociation.com/genres/4000",
                        "http://pressassociation.com/genres/1F0A",
                        "http://pressassociation.com/genres/1F10"
                )
        );

        Set<String> filmTags = paTagMap.map(filmGenres);
        assertEquals(filmTags.size(), 13);
        assertTrue(filmTags.contains("indie"));
        assertTrue(filmTags.contains("romance"));
        assertTrue(filmTags.contains("films"));
        assertTrue(filmTags.contains("horror"));
        assertTrue(filmTags.contains("comedy"));
        assertTrue(filmTags.contains("drama-soap"));
        assertTrue(filmTags.contains("war"));
        assertTrue(filmTags.contains("thriller"));
        assertTrue(filmTags.contains("bollywood-world"));
        assertTrue(filmTags.contains("family"));
        assertTrue(filmTags.contains("sport"));
        assertTrue(filmTags.contains("factual"));
        assertTrue(filmTags.contains("scifi"));
    }

    @Test
    public void testTagMappingForActionFilm() {
        Set<String> actionFilmGenres = new HashSet<>(
                Arrays.asList(
                        "http://pressassociation.com/genres/BF01"
                )
        );

        Set<String> actionFilmTags = paTagMap.map(actionFilmGenres);
        assertEquals(actionFilmTags.size(), 1);
        assertTrue(actionFilmTags.contains("films"));
    }

    @Test
    public void testTagMappingForFactual() {
        Set<String> factualGenres = new HashSet<>(
                Arrays.asList(
                        "http://pressassociation.com/genres/9000",
                        "http://pressassociation.com/genres/7000",
                        "http://pressassociation.com/genres/2200",
                        "http://pressassociation.com/genres/2300",
                        "http://pressassociation.com/genres/2F07",
                        "http://pressassociation.com/genres/2F08",
                        "http://pressassociation.com/genres/8000",
                        "http://pressassociation.com/genres/8200",
                        "http://pressassociation.com/genres/9F05",
                        "http://pressassociation.com/genres/9F00",
                        "http://pressassociation.com/genres/9100",
                        "http://pressassociation.com/genres/9F07",
                        "http://pressassociation.com/genres/9F08",
                        "http://pressassociation.com/genres/9F09",
                        "http://pressassociation.com/genres/9F0A",
                        "http://pressassociation.com/genres/7400",
                        "http://pressassociation.com/genres/9500",
                        "http://pressassociation.com/genres/9200",
                        "http://pressassociation.com/genres/9400"
                )
        );

        Set<String> factualTags = paTagMap.map(factualGenres);
        assertEquals(factualTags.size(), 10);
        assertTrue(factualTags.contains("nature-environment"));
        assertTrue(factualTags.contains("people-society"));
        assertTrue(factualTags.contains("religion-ethics"));
        assertTrue(factualTags.contains("history-biogs"));
        assertTrue(factualTags.contains("learning"));
        assertTrue(factualTags.contains("travel"));
        assertTrue(factualTags.contains("current-affairs-politics"));
        assertTrue(factualTags.contains("arts"));
        assertTrue(factualTags.contains("science-tech"));
        assertTrue(factualTags.contains("factual"));
    }

    @Test
    public void testTagMappingForLifestyle() {
        Set<String> lifestyleGenres = new HashSet<>(
                Arrays.asList(
                        "http://pressassociation.com/genres/A000",
                        "http://pressassociation.com/genres/9F0B",
                        "http://pressassociation.com/genres/A500",
                        "http://pressassociation.com/genres/A400",
                        "http://pressassociation.com/genres/A700",
                        "http://pressassociation.com/genres/AF00",
                        "http://pressassociation.com/genres/AF03",
                        "http://pressassociation.com/genres/AF01",
                        "http://pressassociation.com/genres/A200",
                        "http://pressassociation.com/genres/A100",
                        "http://pressassociation.com/genres/A300",
                        "http://pressassociation.com/genres/AF02",
                        "http://pressassociation.com/genres/A600",
                        "http://pressassociation.com/genres/7B00"
                )
        );

        Set<String> lifestyleTags = paTagMap.map(lifestyleGenres);
        assertEquals(lifestyleTags.size(), 8);
        assertTrue(lifestyleTags.contains("home-garden"));
        assertTrue(lifestyleTags.contains("shopping-fashion"));
        assertTrue(lifestyleTags.contains("leisure-holidays"));
        assertTrue(lifestyleTags.contains("family-friends"));
        assertTrue(lifestyleTags.contains("food-drink"));
        assertTrue(lifestyleTags.contains("health-beauty"));
        assertTrue(lifestyleTags.contains("lifestyle"));
        assertTrue(lifestyleTags.contains("motors-gadgets"));
    }

    @Test
    public void testTagMappingForSport() {
        Set<String> sportGenres = new HashSet<>(
                Arrays.asList(
                        "http://pressassociation.com/genres/4000",
                        "http://pressassociation.com/genres/4F0C",
                        "http://pressassociation.com/genres/4F0D",
                        "http://pressassociation.com/genres/4300",
                        "http://pressassociation.com/genres/4F15",
                        "http://pressassociation.com/genres/4F16",
                        "http://pressassociation.com/genres/4F19",
                        "http://pressassociation.com/genres/4F1D",
                        "http://pressassociation.com/genres/4700",
                        "http://pressassociation.com/genres/4F17",
                        "http://pressassociation.com/genres/4F21",
                        "http://pressassociation.com/genres/4F2B",
                        "http://pressassociation.com/genres/4200",
                        "http://pressassociation.com/genres/4F2D",
                        "http://pressassociation.com/genres/4F2E",
                        "http://pressassociation.com/genres/4F2F",
                        "http://pressassociation.com/genres/4F30",
                        "http://pressassociation.com/genres/4100",
                        "http://pressassociation.com/genres/4F24",
                        "http://pressassociation.com/genres/4400",
                        "http://pressassociation.com/genres/4900",
                        "http://pressassociation.com/genres/4F11"
                )
        );

        Set<String> sportTags = paTagMap.map(sportGenres);
        assertEquals(sportTags.size(), 12);
        assertTrue(sportTags.contains("news-roundups"));
        assertTrue(sportTags.contains("motorsport"));
        assertTrue(sportTags.contains("golf"));
        assertTrue(sportTags.contains("cricket"));
        assertTrue(sportTags.contains("winter-extreme-sports"));
        assertTrue(sportTags.contains("rugby-union"));
        assertTrue(sportTags.contains("horse-racing"));
        assertTrue(sportTags.contains("football"));
        assertTrue(sportTags.contains("sports-events"));
        assertTrue(sportTags.contains("sport"));
        assertTrue(sportTags.contains("rugby-league"));
        assertTrue(sportTags.contains("tennis"));
    }

    @Test
    public void testTagMappingForChildrens() {
        Set<String> childrensGenres = new HashSet<>(
                Arrays.asList(
                        "http://pressassociation.com/genres/5000",
                        "http://pressassociation.com/genres/5500",
                        "http://pressassociation.com/genres/5F00",
                        "http://pressassociation.com/genres/5F01",
                        "http://pressassociation.com/genres/5F02",
                        "http://pressassociation.com/genres/5400",
                        "http://pressassociation.com/genres/5100"
                )
        );

        Set<String> childrensTags = paTagMap.map(childrensGenres);
        assertEquals(childrensTags.size(), 7);
        assertTrue(childrensTags.contains("drama"));
        assertTrue(childrensTags.contains("childrens"));
        assertTrue(childrensTags.contains("pre-school"));
        assertTrue(childrensTags.contains("cartoons"));
        assertTrue(childrensTags.contains("games-quizzes"));
        assertTrue(childrensTags.contains("funnies"));
        assertTrue(childrensTags.contains("kids-learning"));
    }

    @Test
    public void testTagMappingForComedy() {
        Set<String> comedyGenres = new HashSet<>(
                Arrays.asList(
                        "http://pressassociation.com/genres/1400",
                        "http://pressassociation.com/genres/1F01",
                        "http://pressassociation.com/genres/1600",
                        "http://pressassociation.com/genres/1F12",
                        "http://pressassociation.com/genres/3F05"
                )
        );

        Set<String> comedyTags = paTagMap.map(comedyGenres);
        assertEquals(comedyTags.size(), 5);
        assertTrue(comedyTags.contains("romance"));
        assertTrue(comedyTags.contains("sitcom-sketch"));
        assertTrue(comedyTags.contains("comedy"));
        assertTrue(comedyTags.contains("animated"));
        assertTrue(comedyTags.contains("stand-up"));
    }

    @Test
    public void testTagMappingForDramaSoap() {
        Set<String> dramaSoapGenres = new HashSet<>(
                Arrays.asList(
                        "http://pressassociation.com/genres/1000",
                        "http://pressassociation.com/genres/1100",
                        "http://pressassociation.com/genres/1F17",
                        "http://pressassociation.com/genres/1F07",
                        "http://pressassociation.com/genres/1600",
                        "http://pressassociation.com/genres/1300",
                        "http://pressassociation.com/genres/1500"
                )
        );

        Set<String> dramaSoapTags = paTagMap.map(dramaSoapGenres);
        assertEquals(dramaSoapTags.size(), 7);
        assertTrue(dramaSoapTags.contains("romance"));
        assertTrue(dramaSoapTags.contains("medical"));
        assertTrue(dramaSoapTags.contains("drama-soap"));
        assertTrue(dramaSoapTags.contains("historical-period"));
        assertTrue(dramaSoapTags.contains("crime"));
        assertTrue(dramaSoapTags.contains("soap"));
        assertTrue(dramaSoapTags.contains("scifi"));
    }

    @Test
    public void testTagMappingForEntertainment() {
        Set<String> entertainmentGenres = new HashSet<>(
                Arrays.asList(
                        "http://pressassociation.com/genres/3000",
                        "http://pressassociation.com/genres/3300",
                        "http://pressassociation.com/genres/3100",
                        "http://pressassociation.com/genres/3200",
                        "http://pressassociation.com/genres/3F03"
                )
        );

        Set<String> entertainmentTags = paTagMap.map(entertainmentGenres);
        assertEquals(entertainmentTags.size(), 5);
        assertTrue(entertainmentTags.contains("talent-variety"));
        assertTrue(entertainmentTags.contains("entertainment"));
        assertTrue(entertainmentTags.contains("chat-shows"));
        assertTrue(entertainmentTags.contains("games-quizzes"));
        assertTrue(entertainmentTags.contains("celeb-reality"));
    }

    @Test
    public void testTagMappingForMusic() {
        Set<String> musicGenres = new HashSet<>(
                Arrays.asList(
                        "http://pressassociation.com/genres/6000"
                )
        );

        Set<String> musicTags = paTagMap.map(musicGenres);
        assertEquals(musicTags.size(), 1);
        assertTrue(musicTags.contains("music"));
    }

    @Test
    public void testTagMappingForNewsWeather() {
        Set<String> newsWeatherGenres = new HashSet<>(
                Arrays.asList(
                        "http://pressassociation.com/genres/2F02",
                        "http://pressassociation.com/genres/2F03",
                        "http://pressassociation.com/genres/2F04",
                        "http://pressassociation.com/genres/2F05",
                        "http://pressassociation.com/genres/2F06"
                )
        );

        Set<String> newsWeatherTags = paTagMap.map(newsWeatherGenres);
        assertEquals(newsWeatherTags.size(), 1);
        assertTrue(newsWeatherTags.contains("news-weather"));
    }
}
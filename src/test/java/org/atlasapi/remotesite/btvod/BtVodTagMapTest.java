package org.atlasapi.remotesite.btvod;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;

public class BtVodTagMapTest {

    private BtVodTagMap btVodTagMap;

    @Before
    public void setUp() throws Exception {
        this.btVodTagMap = new BtVodTagMap();
    }

    @Test
    public void testFilmMapping() {
        Set<String> filmGenres = new HashSet<>(
                Arrays.asList(
                        "http://vod.bt.com/genres/Film",
                        "http://vod.bt.com/genres/Short Film",
                        "http://vod.bt.com/genres/Action",
                        "http://vod.bt.com/genres/Suspense",
                        "http://vod.bt.com/genres/Family",
                        "http://vod.bt.com/genres/Horror",
                        "http://vod.bt.com/genres/Zombie",
                        "http://vod.bt.com/genres/Monsters",
                        "http://vod.bt.com/genres/Gore",
                        "http://vod.bt.com/genres/Indie",
                        "http://vod.bt.com/genres/Romance",
                        "http://vod.bt.com/genres/Supernatural",
                        "http://vod.bt.com/genres/Sci-Fi",
                        "http://vod.bt.com/genres/Comedy",
                        "http://vod.bt.com/genres/Drama",
                        "http://vod.bt.com/genres/Melodrama",
                        "http://vod.bt.com/genres/Biopic",
                        "http://vod.bt.com/genres/History",
                        "http://vod.bt.com/genres/Thriller",
                        "http://vod.bt.com/genres/War"
                )
        );

        Set<String> filmTags = btVodTagMap.map(filmGenres);
        assertEquals(filmTags.size(), 20);
    }

    @Test
    public void testFactualMapping() {
        Set<String> factualGenres = new HashSet<>(
                Arrays.asList(
                        "http://vod.bt.com/genres/Documentary",
                        "http://vod.bt.com/genres/History",
                        "http://vod.bt.com/genres/Period",
                        "http://vod.bt.com/genres/Biopic",
                        "http://vod.bt.com/genres/Biography",
                        "http://vod.bt.com/genres/Educational",
                        "http://vod.bt.com/genres/Nature",
                        "http://vod.bt.com/genres/People & Culture",
                        "http://vod.bt.com/genres/Cult",
                        "http://vod.bt.com/genres/Science & Technology"
                )
        );

        Set<String> filmTags = btVodTagMap.map(factualGenres);
        assertEquals(filmTags.size(), 10);
    }

    @Test
    public void testLifestyleMapping() {
        Set<String> lifestyleGenres = new HashSet<>(
                Arrays.asList(
                        "http://vod.bt.com/genres/Lifestyle",
                        "http://vod.bt.com/genres/Family",
                        "http://vod.bt.com/genres/Food",
                        "http://vod.bt.com/genres/Home",
                        "http://vod.bt.com/genres/Garden",
                        "http://vod.bt.com/genres/Fashion"
                )
        );

        Set<String> filmTags = btVodTagMap.map(lifestyleGenres);
        assertEquals(filmTags.size(), 6);
    }

    @Test
    public void testSportMapping() {
        Set<String> sportGenres = new HashSet<>(
                Arrays.asList(
                        "http://vod.bt.com/genres/Sport",
                        "http://vod.bt.com/genres/Cricket",
                        "http://vod.bt.com/genres/Football",
                        "http://vod.bt.com/genres/Archive Football",
                        "http://vod.bt.com/genres/Motorsport",
                        "http://vod.bt.com/genres/Moto GP",
                        "http://vod.bt.com/genres/Sports News",
                        "http://vod.bt.com/genres/Rugby",
                        "http://vod.bt.com/genres/Sport",
                        "http://vod.bt.com/genres/Sports Documentary",
                        "http://vod.bt.com/genres/Tennis",
                        "http://vod.bt.com/genres/Extreme Sports",
                        "http://vod.bt.com/genres/Winter Sports"
                )
        );

        Set<String> filmTags = btVodTagMap.map(sportGenres);
        assertEquals(filmTags.size(), 13);
    }

    @Test
    public void testChildrens() {
        Set<String> childrensGenres = new HashSet<>(
                Arrays.asList(
                        "http://vod.bt.com/genres/Junior Girl and Boy",
                        "http://vod.bt.com/genres/Kids",
                        "http://vod.bt.com/genres/Children's",
                        "http://vod.bt.com/genres/Action",
                        "http://vod.bt.com/genres/Adventure",
                        "http://vod.bt.com/genres/Animation",
                        "http://vod.bt.com/genres/Educational",
                        "http://vod.bt.com/genres/Pre-school",
                        "http://vod.bt.com/genres/Preschool"
                )
        );

        Set<String> filmTags = btVodTagMap.map(childrensGenres);
        assertEquals(filmTags.size(), 9);
    }

    @Test
    public void testComedy() {
        Set<String> comedyGenres = new HashSet<>(
                Arrays.asList(
                        "http://vod.bt.com/genres/Animation",
                        "http://vod.bt.com/genres/Anime",
                        "http://vod.bt.com/genres/Romance",
                        "http://vod.bt.com/genres/Sitcoms",
                        "http://vod.bt.com/genres/Stand-Up",
                        "http://vod.bt.com/genres/Teen",
                        "http://vod.bt.com/genres/Youth"
                )
        );

        Set<String> filmTags = btVodTagMap.map(comedyGenres);
        assertEquals(filmTags.size(), 7);
    }

    @Test
    public void testDrama() {
        Set<String> dramaGenres = new HashSet<>(
                Arrays.asList(
                        "http://vod.bt.com/genres/Drama",
                        "http://vod.bt.com/genres/Crime",
                        "http://vod.bt.com/genres/Police",
                        "http://vod.bt.com/genres/Detective",
                        "http://vod.bt.com/genres/History",
                        "http://vod.bt.com/genres/Period",
                        "http://vod.bt.com/genres/Medical",
                        "http://vod.bt.com/genres/Romance",
                        "http://vod.bt.com/genres/Supernatural",
                        "http://vod.bt.com/genres/Sci-Fi",
                        "http://vod.bt.com/genres/Sitcoms",
                        "http://vod.bt.com/genres/Teen"
                )
        );

        Set<String> filmTags = btVodTagMap.map(dramaGenres);
        assertEquals(filmTags.size(), 12);
    }

    @Test
    public void testEntertainment() {
        Set<String> entertainmentGenres = new HashSet<>(
                Arrays.asList(
                        "http://vod.bt.com/genres/Entertainment",
                        "http://vod.bt.com/genres/Talk Show",
                        "http://vod.bt.com/genres/Celebrity",
                        "http://vod.bt.com/genres/Reality"
                )
        );

        Set<String> filmTags = btVodTagMap.map(entertainmentGenres);
        assertEquals(filmTags.size(), 4);
    }

    @Test
    public void testMusic() {
        Set<String> musicGenres = new HashSet<>(
                Arrays.asList(
                        "http://vod.bt.com/genres/Music",
                        "http://vod.bt.com/genres/Karaoke"
                )
        );

        Set<String> filmTags = btVodTagMap.map(musicGenres);
        assertEquals(filmTags.size(), 2);
    }

    @Test
    public void testNews() {
        Set<String> newsGenres = new HashSet<>(
                Arrays.asList(
                        "http://vod.bt.com/genres/News"
                )
        );

        Set<String> filmTags = btVodTagMap.map(newsGenres);
        assertEquals(filmTags.size(), 1);
    }
}
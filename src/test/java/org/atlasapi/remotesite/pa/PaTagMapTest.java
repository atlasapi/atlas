package org.atlasapi.remotesite.pa;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

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
    }

    @Test
    public void testTagMappingForActionFilm() {
        Set<String> filmGenres = new HashSet<>(
                Arrays.asList(
                        "http://pressassociation.com/genres/BF01"
                )
        );

        Set<String> filmTags = paTagMap.map(filmGenres);
        assertEquals(filmTags.size(), 2);
    }

    @Test
    public void testTagMappingForFactual() {
        Set<String> filmGenres = new HashSet<>(
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

        Set<String> filmTags = paTagMap.map(filmGenres);
        assertEquals(filmTags.size(), 19);
    }

    @Test
    public void testTagMappingForLifestyle() {
        Set<String> filmGenres = new HashSet<>(
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

        Set<String> filmTags = paTagMap.map(filmGenres);
        assertEquals(filmTags.size(), 14);
    }

    @Test
    public void testTagMappingForSport() {
        Set<String> filmGenres = new HashSet<>(
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

        Set<String> filmTags = paTagMap.map(filmGenres);
        assertEquals(filmTags.size(), 22);
    }

    @Test
    public void testTagMappingForChildrens() {
        Set<String> filmGenres = new HashSet<>(
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

        Set<String> filmTags = paTagMap.map(filmGenres);
        assertEquals(filmTags.size(), 7);
    }

    @Test
    public void testTagMappingForComedy() {
        Set<String> filmGenres = new HashSet<>(
                Arrays.asList(
                    "http://pressassociation.com/genres/1400",
                    "http://pressassociation.com/genres/1F01",
                    "http://pressassociation.com/genres/1600",
                    "http://pressassociation.com/genres/1F12",
                    "http://pressassociation.com/genres/3F05"
                )
        );

        Set<String> filmTags = paTagMap.map(filmGenres);
        assertEquals(filmTags.size(), 5);
    }

    @Test
    public void testTagMappingForDramaSoap() {
        Set<String> filmGenres = new HashSet<>(
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

        Set<String> filmTags = paTagMap.map(filmGenres);
        assertEquals(filmTags.size(), 7);
    }

    @Test
    public void testTagMappingForEntertainment() {
        Set<String> filmGenres = new HashSet<>(
                Arrays.asList(
                    "http://pressassociation.com/genres/3000",
                    "http://pressassociation.com/genres/3300",
                    "http://pressassociation.com/genres/3100",
                    "http://pressassociation.com/genres/3200",
                    "http://pressassociation.com/genres/3F03"
                )
        );

        Set<String> filmTags = paTagMap.map(filmGenres);
        assertEquals(filmTags.size(), 5);
    }

    @Test
    public void testTagMappingForMusic() {
        Set<String> filmGenres = new HashSet<>(
                Arrays.asList(
                        "http://pressassociation.com/genres/6000"
                )
        );

        Set<String> filmTags = paTagMap.map(filmGenres);
        assertEquals(filmTags.size(), 1);
    }

    @Test
    public void testTagMappingForNewsWeather() {
        Set<String> filmGenres = new HashSet<>(
                Arrays.asList(
                    "http://pressassociation.com/genres/2F02",
                    "http://pressassociation.com/genres/2F03",
                    "http://pressassociation.com/genres/2F04",
                    "http://pressassociation.com/genres/2F05",
                    "http://pressassociation.com/genres/2F06"
                )
        );

        Set<String> filmTags = paTagMap.map(filmGenres);
        assertEquals(filmTags.size(), 5);
    }
}
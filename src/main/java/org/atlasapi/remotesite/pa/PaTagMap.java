package org.atlasapi.remotesite.pa;

import java.util.Map;
import java.util.Set;

import com.google.api.client.util.Maps;
import com.google.api.client.util.Sets;

public class PaTagMap {

    private final Map<String, String> paTagMap = Maps.newHashMap();

    public PaTagMap() {
        mapFilms();
        mapFactual();
        mapLifestyle();
        mapSport();
        mapChildren();
        mapComedy();
        mapDrama();
        mapEntertainment();
        mapMusic();
        mapNews();
    }

    private void mapFilms() {
        paTagMap.put("http://pressassociation.com/genres/BF01", "films");
        paTagMap.put("http://pressassociation.com/genres/1F18", "bollywood-world");
        paTagMap.put("http://pressassociation.com/genres/1F06", "family");
        paTagMap.put("http://pressassociation.com/genres/1F03", "horror");
        paTagMap.put("http://pressassociation.com/genres/1F1A", "indie");
        paTagMap.put("http://pressassociation.com/genres/1600", "romance");
        paTagMap.put("http://pressassociation.com/genres/1300", "scifi");
        paTagMap.put("http://pressassociation.com/genres/1400", "comedy");
        paTagMap.put("http://pressassociation.com/genres/1000", "drama");
        paTagMap.put("http://pressassociation.com/genres/1F19", "factual");
        paTagMap.put( "http://pressassociation.com/genres/4000", "sport");
        paTagMap.put("http://pressassociation.com/genres/1F0A", "thriller");
        paTagMap.put("http://pressassociation.com/genres/1F10", "war");
    }

    private void mapFactual() {
        paTagMap.put("http://pressassociation.com/genres/9000", "factual");
        paTagMap.put("http://pressassociation.com/genres/7000", "arts");
        paTagMap.put("http://pressassociation.com/genres/2200", "current-affairs-politics");
        paTagMap.put("http://pressassociation.com/genres/2300", "current-affairs-politics");
        paTagMap.put("http://pressassociation.com/genres/2F07", "current-affairs-politics");
        paTagMap.put("http://pressassociation.com/genres/2F08", "current-affairs-politics");
        paTagMap.put("http://pressassociation.com/genres/8000", "current-affairs-politics");
        paTagMap.put("http://pressassociation.com/genres/8200", "current-affairs-politics");
        paTagMap.put("http://pressassociation.com/genres/9F05", "history-biogs");
        paTagMap.put("http://pressassociation.com/genres/9F00", "learning");
        paTagMap.put("http://pressassociation.com/genres/9100", "nature-environment");
        paTagMap.put("http://pressassociation.com/genres/9F07", "nature-environment");
        paTagMap.put("http://pressassociation.com/genres/9F08", "nature-environment");
        paTagMap.put("http://pressassociation.com/genres/9F09", "nature-environment");
        paTagMap.put("http://pressassociation.com/genres/9F0A", "nature-environment");
        paTagMap.put("http://pressassociation.com/genres/7400", "people-society");
        paTagMap.put("http://pressassociation.com/genres/9500", "religion-ethics");
        paTagMap.put("http://pressassociation.com/genres/9200", "science-tech");
        paTagMap.put("http://pressassociation.com/genres/9400", "travel");
    }

    private void mapLifestyle() {
        paTagMap.put("http://pressassociation.com/genres/A000", "lifestyle");
        paTagMap.put("http://pressassociation.com/genres/9F0B", "family-friends");
        paTagMap.put("http://pressassociation.com/genres/A500", "food-drink");
        paTagMap.put("http://pressassociation.com/genres/A400", "health-beauty");
        paTagMap.put("http://pressassociation.com/genres/A700", "home-garden");
        paTagMap.put("http://pressassociation.com/genres/AF00", "home-garden");
        paTagMap.put("http://pressassociation.com/genres/AF03", "home-garden");
        paTagMap.put("http://pressassociation.com/genres/AF01", "home-garden");
        paTagMap.put("http://pressassociation.com/genres/A200", "home-garden");
        paTagMap.put("http://pressassociation.com/genres/A100", "leisure-holidays");
        paTagMap.put("http://pressassociation.com/genres/A300", "motors-gadgets");
        paTagMap.put("http://pressassociation.com/genres/AF02", "motors-gadgets");
        paTagMap.put("http://pressassociation.com/genres/A600", "shopping-fashion");
        paTagMap.put("http://pressassociation.com/genres/7B00", "shopping-fashion");
    }

    private void mapSport() {
        paTagMap.put("http://pressassociation.com/genres/4000", "sport");
        paTagMap.put("http://pressassociation.com/genres/4F0C", "cricket");
        paTagMap.put("http://pressassociation.com/genres/4F0D", "cricket");
        paTagMap.put("http://pressassociation.com/genres/4300", "football");
        paTagMap.put("http://pressassociation.com/genres/4F15", "football");
        paTagMap.put("http://pressassociation.com/genres/4F16", "football");
        paTagMap.put("http://pressassociation.com/genres/4F19", "golf");
        paTagMap.put("http://pressassociation.com/genres/4F1D", "horse-racing");
        paTagMap.put("http://pressassociation.com/genres/4700", "motorsport");
        paTagMap.put("http://pressassociation.com/genres/4F17", "motorsport");
        paTagMap.put("http://pressassociation.com/genres/4F21", "motorsport");
        paTagMap.put("http://pressassociation.com/genres/4F2B", "motorsport");
        paTagMap.put("http://pressassociation.com/genres/4200", "news-roundups");
        paTagMap.put("http://pressassociation.com/genres/4F2D", "rugby-league");
        paTagMap.put("http://pressassociation.com/genres/4F2E", "rugby-league");
        paTagMap.put("http://pressassociation.com/genres/4F2F", "rugby-union");
        paTagMap.put("http://pressassociation.com/genres/4F30", "rugby-union");
        paTagMap.put("http://pressassociation.com/genres/4100", "sports-events");
        paTagMap.put("http://pressassociation.com/genres/4F24", "sports-events");
        paTagMap.put("http://pressassociation.com/genres/4400", "tennis");
        paTagMap.put("http://pressassociation.com/genres/4900", "winter-extreme-sports");
        paTagMap.put("http://pressassociation.com/genres/4F11", "winter-extreme-sports");
    }

    private void mapChildren() {
        paTagMap.put("http://pressassociation.com/genres/5000", "childrens");
        paTagMap.put("http://pressassociation.com/genres/5500", "cartoons");
        paTagMap.put("http://pressassociation.com/genres/5F00", "drama");
        paTagMap.put("http://pressassociation.com/genres/5F01", "funnies");
        paTagMap.put("http://pressassociation.com/genres/5F02", "games-quizzes");
        paTagMap.put("http://pressassociation.com/genres/5400", "kids-learning");
        paTagMap.put("http://pressassociation.com/genres/5100", "pre-school");
    }

    private void mapComedy() {
        paTagMap.put("http://pressassociation.com/genres/1400", "comedy");
        paTagMap.put("http://pressassociation.com/genres/1F01", "animated");
        paTagMap.put("http://pressassociation.com/genres/1600", "romcom");
        paTagMap.put("http://pressassociation.com/genres/1F12", "sitcom-sketch");
        paTagMap.put("http://pressassociation.com/genres/3F05", "stand-up");
    }

    private void mapDrama() {
        paTagMap.put("http://pressassociation.com/genres/1000", "drama-soap");
        paTagMap.put("http://pressassociation.com/genres/1100", "crime");
        paTagMap.put("http://pressassociation.com/genres/1F17", "historical-period");
        paTagMap.put("http://pressassociation.com/genres/1F07", "medical");
        paTagMap.put("http://pressassociation.com/genres/1600", "romance");
        paTagMap.put("http://pressassociation.com/genres/1300", "scifi");
        paTagMap.put("http://pressassociation.com/genres/1500", "soap");
    }

    private void mapEntertainment() {
        paTagMap.put("http://pressassociation.com/genres/3000", "entertainment");
        paTagMap.put("http://pressassociation.com/genres/3300", "chat-shows");
        paTagMap.put("http://pressassociation.com/genres/3100", "games-quizzes");
        paTagMap.put("http://pressassociation.com/genres/3200", "talent-variety");
        paTagMap.put("http://pressassociation.com/genres/3F03", "celeb-reality");
    }

    private void mapMusic() {
        paTagMap.put("http://pressassociation.com/genres/6000", "music");
    }

    private void mapNews() {
        paTagMap.put("http://pressassociation.com/genres/2F02", "news-weather");
        paTagMap.put("http://pressassociation.com/genres/2F03", "news-weather");
        paTagMap.put("http://pressassociation.com/genres/2F04", "news-weather");
        paTagMap.put("http://pressassociation.com/genres/2F05", "news-weather");
        paTagMap.put("http://pressassociation.com/genres/2F06", "news-weather");
    }

    public Set<String> map(Set<String> genres) {
        Set<String> tags = Sets.newHashSet();
        for (String genre : genres) {
            if (genre.contains("http://pressassociation.com/genres/")) {
                tags.add(paTagMap.get(genre));
            }
        }

        // Checking if the tags set has only a one tag with the value - film, this means that the
        if (tags.size() == 1 && tags.contains("film")) {
            tags.add("action");
        }
        return tags;
    }
}
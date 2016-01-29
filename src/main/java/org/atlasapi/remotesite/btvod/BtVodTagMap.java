package org.atlasapi.remotesite.btvod;

import java.util.Map;
import java.util.Set;

import com.google.api.client.util.Maps;
import com.google.api.client.util.Sets;

public class BtVodTagMap {

    private final Map<String, String> btVodTagMap = Maps.newHashMap();

    public BtVodTagMap() {
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
        btVodTagMap.put("http://vod.bt.com/genres/Film", "films");
        btVodTagMap.put("http://vod.bt.com/genres/Short Film", "films");
        btVodTagMap.put("http://vod.bt.com/genres/Action", "action");
        btVodTagMap.put("http://vod.bt.com/genres/Suspense", "action");
        btVodTagMap.put("http://vod.bt.com/genres/Family", "family");
        btVodTagMap.put("http://vod.bt.com/genres/Horror", "horror");
        btVodTagMap.put("http://vod.bt.com/genres/Zombie", "horror");
        btVodTagMap.put("http://vod.bt.com/genres/Monsters", "horror");
        btVodTagMap.put("http://vod.bt.com/genres/Gore", "horror");
        btVodTagMap.put("http://vod.bt.com/genres/Indie", "indie");
        btVodTagMap.put("http://vod.bt.com/genres/Romance", "romance");
        btVodTagMap.put("http://vod.bt.com/genres/Supernatural", "scifi");
        btVodTagMap.put("http://vod.bt.com/genres/Sci-Fi", "scifi");
        btVodTagMap.put("http://vod.bt.com/genres/Comedy", "comedy");
        btVodTagMap.put("http://vod.bt.com/genres/Drama", "drama");
        btVodTagMap.put("http://vod.bt.com/genres/Melodrama", "drama");
        btVodTagMap.put("http://vod.bt.com/genres/Biopic", "factual");
        btVodTagMap.put("http://vod.bt.com/genres/History", "factual");
        btVodTagMap.put("http://vod.bt.com/genres/Thriller", "thriller");
        btVodTagMap.put("http://vod.bt.com/genres/War", "war");
    }

    private void mapFactual() {
        btVodTagMap.put("http://vod.bt.com/genres/Documentary", "factual");
        btVodTagMap.put("http://vod.bt.com/genres/History", "history-biogs");
        btVodTagMap.put("http://vod.bt.com/genres/Period", "history-biogs");
        btVodTagMap.put("http://vod.bt.com/genres/Biopic", "history-biogs");
        btVodTagMap.put("http://vod.bt.com/genres/Biography", "history-biogs");
        btVodTagMap.put("http://vod.bt.com/genres/Educational", "learning");
        btVodTagMap.put("http://vod.bt.com/genres/Nature", "nature-environment");
        btVodTagMap.put("http://vod.bt.com/genres/People & Culture", "people-society");
        btVodTagMap.put("http://vod.bt.com/genres/Cult", "religion-ethics");
        btVodTagMap.put("http://vod.bt.com/genres/Science & Technology", "science-tech");
    }

    private void mapLifestyle() {
        btVodTagMap.put("http://vod.bt.com/genres/Lifestyle", "lifestyle");
        btVodTagMap.put("http://vod.bt.com/genres/Family", "family-friends");
        btVodTagMap.put("http://vod.bt.com/genres/Food", "food-drink");
        btVodTagMap.put("http://vod.bt.com/genres/Home", "home-garden");
        btVodTagMap.put("http://vod.bt.com/genres/Garden", "home-garden");
        btVodTagMap.put("http://vod.bt.com/genres/Fashion", "shopping-fashion");
    }

    private void mapSport() {
        btVodTagMap.put("http://vod.bt.com/genres/Sport", "sport");
        btVodTagMap.put("http://vod.bt.com/genres/Cricket", "cricket");
        btVodTagMap.put("http://vod.bt.com/genres/Football", "football");
        btVodTagMap.put("http://vod.bt.com/genres/Archive Football", "football");
        btVodTagMap.put("http://vod.bt.com/genres/Motorsport", "motorsport");
        btVodTagMap.put("http://vod.bt.com/genres/Moto GP", "motorsport");
        btVodTagMap.put("http://vod.bt.com/genres/Sports News", "news-roundups");
        btVodTagMap.put("http://vod.bt.com/genres/Rugby", "rugby-league");
        btVodTagMap.put("http://vod.bt.com/genres/Rugby", "rugby-union");
        btVodTagMap.put("http://vod.bt.com/genres/Sport", "sports-events");
        btVodTagMap.put("http://vod.bt.com/genres/Sports Documentary", "sports-events");
        btVodTagMap.put("http://vod.bt.com/genres/Tennis", "tennis");
        btVodTagMap.put("http://vod.bt.com/genres/Extreme Sports", "winter-extreme-sports");
        btVodTagMap.put("http://vod.bt.com/genres/Winter Sports", "winter-extreme-sports");
    }

    private void mapChildren() {
        btVodTagMap.put("http://vod.bt.com/genres/Junior Girl and Boy", "childrens");
        btVodTagMap.put("http://vod.bt.com/genres/Kids", "childrens");
        btVodTagMap.put("http://vod.bt.com/genres/Children's", "childrens");
        btVodTagMap.put("http://vod.bt.com/genres/Action", "action-adventure");
        btVodTagMap.put("http://vod.bt.com/genres/Adventure", "action-adventure");
        btVodTagMap.put("http://vod.bt.com/genres/Animation", "cartoons");
        btVodTagMap.put("http://vod.bt.com/genres/Educational", "kids-learning");
        btVodTagMap.put("http://vod.bt.com/genres/Pre-school", "pre-school");
        btVodTagMap.put("http://vod.bt.com/genres/Preschool", "pre-school");
    }

    private void mapComedy() {
        btVodTagMap.put("http://vod.bt.com/genres/Animation", "animated");
        btVodTagMap.put("http://vod.bt.com/genres/Anime", "animated");
        btVodTagMap.put("http://vod.bt.com/genres/Romance", "romcom");
        btVodTagMap.put("http://vod.bt.com/genres/Sitcoms", "sitcom-sketch");
        btVodTagMap.put("http://vod.bt.com/genres/Stand-Up", "stand-up");
        btVodTagMap.put("http://vod.bt.com/genres/Teen", "teen");
        btVodTagMap.put("http://vod.bt.com/genres/Youth", "teen");
    }

    private void mapDrama() {
        btVodTagMap.put("http://vod.bt.com/genres/Drama", "drama-soap");
        btVodTagMap.put("http://vod.bt.com/genres/Crime", "crime");
        btVodTagMap.put("http://vod.bt.com/genres/Police", "crime");
        btVodTagMap.put("http://vod.bt.com/genres/Detective", "crime");
        btVodTagMap.put("http://vod.bt.com/genres/History", "historical-period");
        btVodTagMap.put("http://vod.bt.com/genres/Period", "historical-period");
        btVodTagMap.put("http://vod.bt.com/genres/Medical", "medical");
        btVodTagMap.put("http://vod.bt.com/genres/Romance", "romance");
        btVodTagMap.put("http://vod.bt.com/genres/Supernatural", "scifi");
        btVodTagMap.put("http://vod.bt.com/genres/Sci-Fi", "scifi");
        btVodTagMap.put("http://vod.bt.com/genres/Sitcoms", "soap");
        btVodTagMap.put("http://vod.bt.com/genres/Teen", "teen");
    }

    private void mapEntertainment() {
        btVodTagMap.put("http://vod.bt.com/genres/Entertainment", "entertainment");
        btVodTagMap.put("http://vod.bt.com/genres/Talk Show", "chat-shows");
        btVodTagMap.put("http://vod.bt.com/genres/Celebrity", "celeb-reality");
        btVodTagMap.put("http://vod.bt.com/genres/Reality", "celeb-reality");

    }

    private void mapMusic() {
        btVodTagMap.put("http://vod.bt.com/genres/Music", "music");
        btVodTagMap.put("http://vod.bt.com/genres/Karaoke", "music");
    }

    private void mapNews() {
        btVodTagMap.put("http://vod.bt.com/genres/News", "news-weather");
    }

    public Set<String> map(Set<String> genres) {
        Set<String> tags = Sets.newHashSet();
        for (String genre : genres) {
            if (genre.contains("http://vod.bt.com/genres/")) {
                tags.add(btVodTagMap.get(genre));
            }
        }

        return tags;
    }
}

package org.atlasapi.remotesite.wikipedia.television;

import java.util.Map;
import java.util.NavigableMap;

import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Series;
import org.atlasapi.remotesite.ContentExtractor;
import org.atlasapi.remotesite.wikipedia.wikiparsers.Article;
import org.atlasapi.remotesite.wikipedia.wikiparsers.SwebleHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

public class TvBrandHierarchyExtractor implements ContentExtractor<ScrapedFlatHierarchy, TvBrandHierarchy> {
    private static final Logger log = LoggerFactory.getLogger(TvBrandHierarchyExtractor.class);

    @Override
    public TvBrandHierarchy extract(ScrapedFlatHierarchy source) {
        NavigableMap<Integer, Episode> episodes = Maps.newTreeMap();
        Map<String, Series> seasons = Maps.newTreeMap();
        
        Article brandArticle = source.getBrandArticle();
        Brand brand = extractBrand(brandArticle, source.getBrandInfo());
        
        for(ScrapedEpisode scrapedEpisode : source.getEpisodes()) {
            String seasonName = scrapedEpisode.season == null ? null : Strings.emptyToNull(scrapedEpisode.season.name);
            Series season = seasonName == null ? null : getSeason(seasons, seasonName, brand);
            
            int episodeNumber = scrapedEpisode.numberInShow;
            if (episodeNumber == 0) {
                log.warn("The TV show " + brand.getTitle() + " has an episode without a number, which is being ignored.");
                continue;
            }
            Episode episode = episodes.get(scrapedEpisode.numberInShow);
            if(episode == null) {
                episode = new Episode(brand.getCanonicalUri() + ":ep:" + episodeNumber, null, Publisher.WIKIPEDIA);
                episode.setEpisodeNumber(scrapedEpisode.numberInSeason);
                episodes.put(episodeNumber, episode);
                // TODO somehow extract (accurate, final) episode count for each season (better not to guess!)
            }
            
            episode.setContainer(brand);  // TODO this should be done only after writing and re-resolving the brand
            if (season != null) {  // in case we scraped the same episode both within and without a season, we don't want to stupidly wipe out the series ref during this merge!
                episode.setSeries(season);  // TODO this should be done only after writing and re-resolving the season
            }
            episode.setTitle(Strings.emptyToNull(scrapedEpisode.title));
        }
        
        return new TvBrandHierarchy(brand, ImmutableSet.copyOf(seasons.values()), ImmutableSet.copyOf(episodes.values()));
    }

    private Brand extractBrand(Article brandArticle, ScrapedBrandInfobox info) {
        String url = brandArticle.getUrl();
        String title = null;
        Brand brand = new Brand(url, null, Publisher.WIKIPEDIA);
        
        if (info == null) {
            log.warn("Extracting Brand info seems to have failed on \"" + brandArticle.getTitle() + "\"");
            return brand;
        }
        
        title = Strings.emptyToNull(info.title);
        if (title == null) {
            log.info("Falling back to guessing brand name from \"" + brandArticle.getTitle() + "\"");
            title = guessBrandNameFromArticleTitle(brandArticle.getTitle());
        }
        brand.setTitle(title);
        if (!Strings.isNullOrEmpty(info.image)) {
            String image = SwebleHelper.getWikiImage(info.image);
            brand.setImage(image);
        }

        if (info.firstAired.isPresent()) {
            brand.setYear(info.firstAired.get().getYear());
        }
        if (info.imdbID != null) {
            brand.addAlias(new Alias("imdb:title", info.imdbID));
            brand.addAlias(new Alias("imdb:url", "http://imdb.com/title/tt" + info.imdbID));
        }
        return brand;
    }
    
    private String guessBrandNameFromArticleTitle(String title) {
        int indexOfBracketedStuffWeDontWant = title.indexOf(" (");
        if (indexOfBracketedStuffWeDontWant == -1) {  // nothing to discard
            return title;
        } else {
            return title.substring(0, indexOfBracketedStuffWeDontWant);
        }
    }

    /**
     * Returns the season object for the given season name from the map, or if it isn't there, makes one to the best of our ability, adds it and then returns it.
     */
    private Series getSeason(Map<String, Series> seasons, String name, Brand brand) {
        Series season = seasons.get(name);
        if (season == null) {
            season = new Series(brand.getCanonicalUri() + ":se:" + name, null, Publisher.WIKIPEDIA);
            seasons.put(name, season);
        }
        season.setTitle(name);
        season.setParent(brand);  // TODO this should be done only after writing and re-resolving the brand
        return season;
    }
    
}

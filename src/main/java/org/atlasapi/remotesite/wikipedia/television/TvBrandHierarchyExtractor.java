package org.atlasapi.remotesite.wikipedia.television;

import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.metabroadcast.common.intl.Countries;
import com.metabroadcast.common.intl.Country;
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

//    YYYY/MM/DD (sometimes only one M and/or D present)
    private static Pattern YEAR_PATTERN_1 = Pattern.compile(".*first_aired.*?[sS]tart date[|](\\d{4})[|](\\d{1,2})[|](\\d{1,2}).*", Pattern.DOTALL);

    private static Pattern US_PATTERN = Pattern.compile(".*[|] country.*?(United States).*", Pattern.DOTALL);
    private static Pattern UK_PATTERN = Pattern.compile(".*[|] country.*?(United Kingdom).*", Pattern.DOTALL);

    @Override
    public TvBrandHierarchy extract(ScrapedFlatHierarchy source) {
        log.info(source.getBrandArticle().getMediaWikiSource());
        NavigableMap<Integer, Episode> episodes = Maps.newTreeMap();
        Map<String, Series> seasons = Maps.newTreeMap();
        
        Article brandArticle = source.getBrandArticle();
        Brand brand = extractBrand(brandArticle, source);
        
        for(ScrapedEpisode scrapedEpisode : source.getEpisodes()) {
            String seasonName = scrapedEpisode.season == null ? null : Strings.emptyToNull(scrapedEpisode.season.name);
            Series season = seasonName == null ? null : getSeason(seasons, seasonName, brand);
            
            int episodeNumber = scrapedEpisode.numberInShow;
            if (episodeNumber == 0) {
                log.warn("The TV show {} has an episode without a number, which is being ignored.", brand.getTitle());
                continue;
            }
            Episode episode = episodes.get(scrapedEpisode.numberInShow);
            if(episode == null) {
                episode = new Episode(brand.getCanonicalUri() + ":ep:" + episodeNumber, null, Publisher.WIKIPEDIA);
                episode.setEpisodeNumber(scrapedEpisode.numberInSeason);
                episodes.put(episodeNumber, episode);
            }
            
            episode.setContainer(brand);
            if (season != null) {  // in case we scraped the same episode both within and without a season, we don't want to stupidly wipe out the series ref during this merge!
                episode.setSeries(season);
            }
            episode.setTitle(Strings.emptyToNull(scrapedEpisode.title));
        }
        
        return new TvBrandHierarchy(brand, ImmutableSet.copyOf(seasons.values()), ImmutableSet.copyOf(episodes.values()));
    }

    private Brand extractBrand(Article brandArticle, ScrapedFlatHierarchy info) {
        ScrapedBrandInfobox brandInfo = info.getBrandInfo();

        String url = brandArticle.getUrl();
        Brand brand = new Brand(url, null, Publisher.WIKIPEDIA);
        
        if (brandInfo == null) {
            log.warn("Extracting Brand info seems to have failed on {}", brandArticle.getTitle());
            return brand;
        }

        String title = brandInfo.title;

        if (Strings.isNullOrEmpty(title)) {
            log.info("Falling back to guessing brand name from {}", brandArticle.getTitle());
            title = guessBrandNameFromArticleTitle(brandArticle.getTitle());
        }

        brand.setTitle(title);

        if (!Strings.isNullOrEmpty(brandInfo.image)) {
            String image = SwebleHelper.getWikiImage(brandInfo.image);
            brand.setImage(image);
        }

        if (brandInfo.firstAired.isPresent()) {
            brand.setYear(brandInfo.firstAired.get().getYear());
        } else {
            attemptYearFromSource(brand, info);
        }

        if (brandInfo.imdbID != null) {
            brand.addAlias(new Alias("imdb:title", brandInfo.imdbID));
            brand.addAlias(new Alias("imdb:url", "http://imdb.com/title/tt" + brandInfo.imdbID));
        }

        brand.setCountriesOfOrigin(extractCountriesSource(info));

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
     * Only grabs the most common pattern of start date. Room for improvement.
     */
    private void attemptYearFromSource(Brand brand, ScrapedFlatHierarchy info) {
        Matcher matcher = YEAR_PATTERN_1.matcher(info.getBrandArticle().getMediaWikiSource());
        if (matcher.matches()) {
            Integer year = Integer.parseInt(matcher.group(1));
            brand.setYear(year);
        }
    }

    /**
     * Only grabs the easiest to parse US and UK countries. Room for improvement.
     */
    private Set<Country> extractCountriesSource(ScrapedFlatHierarchy info) {
        Matcher usMatcher = US_PATTERN.matcher(info.getBrandArticle().getMediaWikiSource());
        if (usMatcher.matches()) {
            return ImmutableSet.of(Countries.US);
        }

        Matcher ukMatcher = UK_PATTERN.matcher(info.getBrandArticle().getMediaWikiSource());
        if (ukMatcher.matches()) {
            return ImmutableSet.of(Countries.GB);
        }

        return ImmutableSet.of();
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
        season.setParent(brand);
        return season;
    }
    
}

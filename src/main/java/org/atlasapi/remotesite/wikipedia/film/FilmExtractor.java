package org.atlasapi.remotesite.wikipedia.film;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.google.common.collect.Lists;
import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.CrewMember;
import org.atlasapi.media.entity.CrewMember.Role;
import org.atlasapi.media.entity.Film;
import org.atlasapi.media.entity.Image;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.ReleaseDate;
import org.atlasapi.media.entity.ReleaseDate.ReleaseType;
import org.atlasapi.media.entity.Version;
import org.atlasapi.remotesite.ContentExtractor;
import org.atlasapi.remotesite.wikipedia.wikiparsers.Article;
import org.atlasapi.remotesite.wikipedia.wikiparsers.SwebleHelper;
import org.atlasapi.remotesite.wikipedia.wikiparsers.SwebleHelper.ListItemResult;
import org.atlasapi.remotesite.wikipedia.film.FilmInfoboxScraper.ReleaseDateResult;
import org.atlasapi.remotesite.wikipedia.film.FilmInfoboxScraper.Result;
import org.joda.time.Duration;
import org.joda.time.LocalDate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import xtc.parser.ParseException;

import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.intl.Countries;
import com.metabroadcast.common.intl.Country;

/**
 * This attempts to extract a {@link Film} from its Wikipedia article.
 */
public class FilmExtractor implements ContentExtractor<Article, Film> {
    private static final Logger log = LoggerFactory.getLogger(FilmExtractor.class);

    @Override
    public Film extract(Article article) {
        String source = article.getMediaWikiSource();
        try {
            Result info = FilmInfoboxScraper.getInfoboxAttrs(source);
            
            String url = article.getUrl();
            Film flim = new Film(url, url, Publisher.WIKIPEDIA);
            
            flim.setLastUpdated(article.getLastModified());
            
            ImmutableList<ListItemResult> title = info.name;
            if (title != null && title.size() == 1) {
                flim.setTitle(title.get(0).name);
            } else {
                log.info("Film in Wikipedia article \"" + article.getTitle() + "\" has " + (title == null || title.isEmpty() ? "no title." : "multiple titles.") + " Falling back to guessing from article title.");
                flim.setTitle(guessFilmNameFromArticleTitle(article.getTitle()));
            }
            
            List<CrewMember> people = flim.getPeople();
            crewify(info.cinematographers, Role.DIRECTOR_OF_PHOTOGRAPHY, people);
            crewify(info.composers, Role.COMPOSER, people);
            crewify(info.directors, Role.DIRECTOR, people);
            crewify(info.editors, Role.EDITOR, people);
            crewify(info.narrators, Role.NARRATOR, people);
            crewify(info.producers, Role.PRODUCER, people);
            crewify(info.writers, Role.WRITER, people);
            crewify(info.storyWriters, Role.SOURCE_WRITER, people);
            crewify(info.screenplayWriters, Role.ADAPTED_BY, people);
            crewify(info.starring, Role.ACTOR, people);
            
            if (info.externalAliases != null) {
                for (Map.Entry<String, String> a : info.externalAliases.entrySet()) {
                    flim.addAlias(new Alias(a.getKey(), a.getValue()));
                }
            }
            
            if (info.releaseDates != null) {
                ImmutableSet.Builder<ReleaseDate> releaseDates = ImmutableSet.builder();
                for (ReleaseDateResult result : info.releaseDates) {
                    Optional<ReleaseDate> releaseDate = extractReleaseDate(result);
                    if (releaseDate.isPresent()) {
                        releaseDates.add(releaseDate.get());
                    }
                }
                flim.setReleaseDates(releaseDates.build());
            }
            
            if (info.runtimeInMins != null && info.runtimeInMins > 0) {
                Version v = new Version();
                v.setDuration(new Duration(info.runtimeInMins * 60000));
                flim.addVersion(v);
            }


            if (info.image != null) {
                Image image = new Image(SwebleHelper.getWikiImage(info.image));
                flim.setImages(ImmutableList.of(image));
            }
            
            return flim;
        } catch (IOException | ParseException ex) {
            throw new RuntimeException(ex);
        }
    }
    
    private String guessFilmNameFromArticleTitle(String title) {
        int indexOfBracketedStuffWeDontWant = title.indexOf(" (");
        if (indexOfBracketedStuffWeDontWant == -1) {  // nothing to discard
            return title;
        } else {
            return title.substring(0, indexOfBracketedStuffWeDontWant);
        }
    }

    private void crewify(ImmutableList<ListItemResult> from, Role role, List<CrewMember> into) {
        if (from == null) {
            return;
        }

        for (ListItemResult person : from) {
            if (person.articleTitle.isPresent()) {
                into.add(new CrewMember(Article.urlFromTitle(person.articleTitle.get()), null, Publisher.WIKIPEDIA).withRole(role).withName(person.name));
            } else {
                into.add(new CrewMember().withRole(role).withName(person.name).withPublisher(Publisher.WIKIPEDIA));
            }
        }
    }
    
    private static final Map<String, Country> countryNames = new TreeMap<String, Country>(){{
        put("united kingdom",   Countries.GB);
        put("uk",               Countries.GB);
        put("britain",          Countries.GB);
        put("ireland",          Countries.IE);
        put("us",               Countries.US);
        put("united states",    Countries.US);
        put("usa",              Countries.US);
        put("america",          Countries.US);
        put("france",           Countries.FR);
        put("italy",            Countries.IT);
    }};
    
    private Optional<ReleaseDate> extractReleaseDate(ReleaseDateResult result) {
        ReleaseType type = ReleaseType.GENERAL;

        LocalDate date;
        try {
            date = new LocalDate(Integer.parseInt(result.year()), Integer.parseInt(result.month()), Integer.parseInt(result.day()));
        } catch (Exception e) {
            log.warn("Failed to interpret release date \"" + result.year() + "|" + result.month() + "|" + result.day() + "\" – ignoring release date");
            return Optional.absent();
        }

        Country country;
        if (result.location() == null || Strings.isNullOrEmpty(result.location().name)) {
            country = Countries.ALL;
        } else {
            String location = result.location().name.trim().toLowerCase();
            country = countryNames.get(location);
            if (country == null) {  // If we can't recognize it, a) it can't be represented, and b) it's probably a festival or something.
                log.warn("Failed to interpret release location \"" + location + "\" – ignoring release date");
                return Optional.absent();
            }
        }
        
        return Optional.of(new ReleaseDate(date, country, type));
    }
}

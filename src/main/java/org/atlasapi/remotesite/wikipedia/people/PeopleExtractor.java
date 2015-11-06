package org.atlasapi.remotesite.wikipedia.people;

import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Organisation;
import org.atlasapi.media.entity.Person;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.remotesite.ContentExtractor;
import org.atlasapi.remotesite.wikipedia.wikiparsers.Article;
import org.atlasapi.remotesite.wikipedia.wikiparsers.SwebleHelper;
import org.atlasapi.remotesite.wikipedia.football.TeamInfoboxScrapper;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xtc.parser.ParseException;

import java.io.IOException;

import com.google.api.client.util.Strings;

public class PeopleExtractor implements ContentExtractor<Article, Person> {
    private static final Logger log = LoggerFactory.getLogger(PeopleExtractor.class);

    @Override
    public Person extract(Article article) {
        String source = article.getMediaWikiSource();
        try {
            PeopleInfoboxScrapper.Result info = PeopleInfoboxScrapper.getInfoboxAttrs(source);
            String url = article.getUrl();
            Person person = new Person(url, url, Publisher.WIKIPEDIA);
            person.setLastUpdated(article.getLastModified());
            person.setBirthPlace(info.birthPlace);
            if (!Strings.isNullOrEmpty(info.name)) {
                person.setTitle(info.name);
            } else {
                person.setTitle(article.getTitle());
            }
            if (!Strings.isNullOrEmpty(info.alias)) {
                person.addAlias(new Alias("imdb:uri", info.alias));
            }
            if (!Strings.isNullOrEmpty(info.image)) {
                person.setImage(SwebleHelper.getWikiImage(info.image));
            }
            if (!Strings.isNullOrEmpty(info.birthDate)) {
                person.setBirthDate(parseBirthDate(info.birthDate));
            }
            if (!Strings.isNullOrEmpty(info.fullname)) {
                person = person.withName(info.fullname);
            }
            if (!Strings.isNullOrEmpty(info.website)) {
                person = person.withProfileLink(info.website);
            }
            return person;
        } catch (IOException | ParseException ex) {
            throw new RuntimeException(ex);
        }
    }

    private DateTime parseBirthDate(String date) {
        DateTimeFormatter formatter = DateTimeFormat.forPattern("MMM dd, yyyy");
        DateTime dt = formatter.parseDateTime(date);
        return dt;
    }

}

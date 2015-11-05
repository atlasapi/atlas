package org.atlasapi.remotesite.wikipedia.people;

import org.atlasapi.media.entity.Person;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.remotesite.ContentExtractor;
import org.atlasapi.remotesite.wikipedia.wikiparsers.Article;
import org.atlasapi.remotesite.wikipedia.wikiparsers.SwebleHelper;
import org.atlasapi.remotesite.wikipedia.football.TeamInfoboxScrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xtc.parser.ParseException;

import java.io.IOException;

public class PeopleExtractor implements ContentExtractor<Article, Person> {
    private static final Logger log = LoggerFactory.getLogger(PeopleExtractor.class);

    @Override
    public Person extract(Article article) {
        String source = article.getMediaWikiSource();
        //TODO implement extracting
        String url = article.getUrl();
        Person person = new Person();
        person.setPublisher(Publisher.WIKIPEDIA);
        person.setLastUpdated(article.getLastModified());
        person.setCanonicalUri(url);
        return person;
    }

}

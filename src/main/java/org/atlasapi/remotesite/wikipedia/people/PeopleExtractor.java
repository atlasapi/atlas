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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xtc.parser.ParseException;

import java.io.IOException;

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
            if ( info.birthPlace != null) {
                person.setBirthPlace(info.birthPlace);
            }
            if (info.fullname != null) {
                person.withName(info.fullname);
            }
            if (info.name != null) {
                person.setTitle(info.name);
            } else {
                person.setTitle(article.getTitle());
            }
            if (info.alias != null) {
                person.addAlias(new Alias("imdb:uri", info.alias));
            }
            if (info.website != null) {
                person.withProfileLink(info.website);
            }
            if (info.image != null) {
                person.setImage(SwebleHelper.getWikiImage(info.image));
            }
            return person;
        } catch (IOException | ParseException ex) {
            throw new RuntimeException(ex);
        }

    }

}

package org.atlasapi.remotesite.wikipedia;

import com.metabroadcast.common.time.DateTimeZones;
import com.mongodb.DBObject;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.MediaType;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.mongo.MongoPersonStore;
import org.atlasapi.persistence.content.people.PersonWriter;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.remotesite.pa.listings.bindings.Person;
import org.atlasapi.remotesite.wikipedia.film.FilmExtractor;
import org.atlasapi.remotesite.wikipedia.people.PeopleExtractor;
import org.atlasapi.remotesite.wikipedia.testutils.LocallyCachingArticleFetcher;
import org.atlasapi.remotesite.wikipedia.updaters.FilmsUpdater;
import org.atlasapi.remotesite.wikipedia.updaters.PeopleUpdater;
import org.atlasapi.remotesite.wikipedia.wikiparsers.FetchMeister;
import org.joda.time.DateTime;

public class RunPeopleExtraction {
    public static void main(String... args) {
        EnglishWikipediaClient ewc = new EnglishWikipediaClient();
        new PeopleUpdater(
                ewc,
                new FetchMeister(new LocallyCachingArticleFetcher(ewc, System.getProperty("user.home") + "/atlasTestCaches/wikipedia/people")),
                new PeopleExtractor(),
                new PersonWriter() {
                    @Override
                    public void updatePersonItems(org.atlasapi.media.entity.Person person) {

                    }

                    @Override
                    public void createOrUpdatePerson(org.atlasapi.media.entity.Person person) {
                       
                    }
                },
                5,
                2
        ).run();
    }
}

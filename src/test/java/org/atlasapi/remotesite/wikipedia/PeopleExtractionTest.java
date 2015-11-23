package org.atlasapi.remotesite.wikipedia;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Collection;

import org.apache.commons.io.IOUtils;
import org.atlasapi.media.entity.Person;
import org.atlasapi.remotesite.wikipedia.film.FilmExtractor;
import org.atlasapi.remotesite.wikipedia.football.EuropeanTeamListScraper;
import org.atlasapi.remotesite.wikipedia.people.ActorsNamesListScrapper;
import org.atlasapi.remotesite.wikipedia.people.FootballListScrapper;
import org.atlasapi.remotesite.wikipedia.people.PeopleExtractor;
import org.atlasapi.remotesite.wikipedia.wikiparsers.Article;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.collect.Iterables;
import com.google.common.io.Resources;

import xtc.parser.ParseException;

public class PeopleExtractionTest {
    private PeopleExtractor extractor;

    private Article fakeArticle(final String articleText) {
        return fakeArticle("Fake title", articleText);
    }
    private Article fakeArticle(final String title, final String articleText) {
        return new Article() {
            @Override
            public DateTime getLastModified() {
                return new DateTime();
            }

            @Override
            public String getMediaWikiSource() {
                return articleText;
            }

            @Override
            public String getTitle() {
                return title;
            }
        };
    }

    @Before
    public void setUp() {
        extractor = new PeopleExtractor();
    }

    @Test
    public void testThatListScrappedCorrectly() throws IOException, ParseException {
        Collection<String> names = FootballListScrapper.extractOneList(
                IOUtils.toString(Resources.getResource(getClass(),
                        "people/List of Albania international footballers.mediawiki").openStream(), Charsets.UTF_8.name()));
        assertTrue(names.size()==243);
    }

    @Test
    public void testFabregasArticleExtraction() throws IOException, ParseException {
        Person fabregas = extractor.extract(fakeArticle(
                IOUtils.toString(Resources.getResource(getClass(), "people/Cesc Fàbregas.mediawiki").openStream(), Charsets.UTF_8.name())
        ));
        assertEquals(new DateTime(1987,5,4,0,0,0),fabregas.getBirthDate());
        assertEquals("Arenys de Mar, Spain", fabregas.getBirthPlace());
        assertTrue(fabregas.profileLinks().isEmpty());
        assertEquals("Cesc Fàbregas",fabregas.getTitle());
        assertTrue(fabregas.getAliases().isEmpty());
        assertEquals("http://upload.wikimedia.org/wikipedia/commons/b/b4/Cesc_Fabregas_vs_Maccabi_Tel-Aviv%2C_Sep_2015.jpg", Iterables.getOnlyElement(fabregas.getImages()).getCanonicalUri());
        assertEquals("Cesc Fàbregas",fabregas.name());
    }
}

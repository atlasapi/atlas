package org.atlasapi.remotesite.wikipedia;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.CrewMember;
import org.atlasapi.media.entity.CrewMember.Role;
import org.atlasapi.media.entity.Film;
import org.atlasapi.media.entity.ReleaseDate;
import org.atlasapi.media.entity.ReleaseDate.ReleaseType;
import org.atlasapi.remotesite.wikipedia.film.FilmExtractor;
import org.atlasapi.remotesite.wikipedia.wikiparsers.Article;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Resources;
import com.metabroadcast.common.intl.Countries;

public class FilmExtractionTests {
    FilmExtractor extractor;
    String articleText;
    Article article;
    
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
    
    private static class CrewMemberTestFields {
        public String name;
        public Role role;
        public String articleTitle;
        public CrewMemberTestFields(String name, Role role) {
            this(name, role, name);
        }
        public CrewMemberTestFields(String name, Role role, String articleTitle) {
            this.name = name;
            this.role = role;
            this.articleTitle = articleTitle;
        }
    }
    
    private boolean matches(CrewMemberTestFields assertion, CrewMember person) {
        if (!( assertion.name.equals(person.name()) && assertion.role == person.role() )) {
            return false;
        }
        assertEquals(assertion.articleTitle == null ? null : Article.urlFromTitle(assertion.articleTitle), person.getCanonicalUri());
        return true;
    }
    
    private void assertAllPresentAndCorrect(List<CrewMember> actuallyExtracted, List<CrewMemberTestFields> shouldBe) {
        try {
            assertions:
            for (CrewMemberTestFields assertion : shouldBe) {
                for (CrewMember person : actuallyExtracted) {
                    if (matches(assertion, person)) { continue assertions; }
                }
                fail("Extraction of " + assertion.name + " seems to have failed.");
            }
            people:
            for (CrewMember person : actuallyExtracted) {
                for (CrewMemberTestFields assertion : shouldBe) {
                   if (matches(assertion, person)) { continue people; }
                }
                fail("Extracted unexpected " + person + " (" + person.name() + ")!");
            }
        } catch (AssertionError e) {
            System.err.println("Actually extracted these:");
            for (CrewMember person : actuallyExtracted) {
                System.err.println("  " + person.name() + ", " + person.role());
            }
            throw e;
        }
    }
    
    private void assertReleaseDatesEqual(Collection<ReleaseDate> extracted, Collection<ReleaseDate> expected) {
        try {
            assertEquals(expected.size(), extracted.size());
            for (ReleaseDate r : expected) {
                assertTrue(extracted.contains(r));
            }
        } catch (AssertionError e) {
            System.out.println("Actually extracted these:");
            for (ReleaseDate d : extracted) {
                System.out.println("  " + d.date() + " (" + d.country().code() + ")");
            }
            throw e;
        }
    }
    
    @Before
    public void setUp() throws IOException {
        extractor = new FilmExtractor();
    }
    
    @Test
    public void testHackers() throws IOException {
        Film flim = extractor.extract(fakeArticle(
                IOUtils.toString(Resources.getResource(getClass(), "film/Hackers.mediawiki").openStream(), Charsets.UTF_8.name())
        ));
        assertEquals("Hackers", flim.getTitle());
        
        assertTrue(flim.getAliases().contains(new Alias("imdb:url", "http://imdb.com/title/tt0113243")));
        assertTrue(flim.getAliases().contains(new Alias("imdb:title", "0113243")));
        assertTrue(flim.getAliases().contains(new Alias("rottentomatoes:movie", "hackers")));
        assertTrue(flim.getAliases().contains(new Alias("metacritic:movie", "hackers")));
        assertTrue(flim.getAliases().contains(new Alias("boxofficemojo:movie", "hackers")));
        
        assertAllPresentAndCorrect(flim.getPeople(), ImmutableList.of(
                new CrewMemberTestFields("Iain Softley", Role.DIRECTOR),
                new CrewMemberTestFields("Michael Peyser", Role.PRODUCER, null),
                new CrewMemberTestFields("Rafael Moreu", Role.WRITER),
                new CrewMemberTestFields("Simon Boswell", Role.COMPOSER),
                new CrewMemberTestFields("Chris Blunden", Role.EDITOR, null),
                new CrewMemberTestFields("Martin Walsh", Role.EDITOR, "Martin Walsh (film editor)"),
                new CrewMemberTestFields("Jonny Lee Miller", Role.ACTOR),
                new CrewMemberTestFields("Angelina Jolie", Role.ACTOR),
                new CrewMemberTestFields("Jesse Bradford", Role.ACTOR),
                new CrewMemberTestFields("Matthew Lillard", Role.ACTOR),
                new CrewMemberTestFields("Fisher Stevens", Role.ACTOR),
                new CrewMemberTestFields("Lorraine Bracco", Role.ACTOR),
                new CrewMemberTestFields("Renoly Santiago", Role.ACTOR),
                new CrewMemberTestFields("Laurence Mason", Role.ACTOR)
        ));
        
        assertReleaseDatesEqual(flim.getReleaseDates(), ImmutableSet.of(
                new ReleaseDate(new LocalDate(1995, 9, 15), Countries.ALL, ReleaseType.GENERAL)
        ));
    }
    
    @Test
    public void testRobinHood() throws IOException {
        Film flim = extractor.extract(fakeArticle("The Adventures of Robin Hood (film)",
                IOUtils.toString(Resources.getResource(getClass(), "film/The Adventures of Robin Hood (film).mediawiki").openStream(), Charsets.UTF_8.name())
        ));
        assertEquals("The Adventures of Robin Hood", flim.getTitle());
        
        assertTrue(flim.getAliases().contains(new Alias("imdb:url", "http://imdb.com/title/tt0029843")));
        assertTrue(flim.getAliases().contains(new Alias("imdb:title", "0029843")));
        
        assertAllPresentAndCorrect(flim.getPeople(), ImmutableList.of(
                new CrewMemberTestFields("Michael Curtiz", Role.DIRECTOR),
                new CrewMemberTestFields("William Keighley", Role.DIRECTOR),
                new CrewMemberTestFields("Hal B. Wallis", Role.PRODUCER),
                new CrewMemberTestFields("Henry Blanke", Role.PRODUCER),
                new CrewMemberTestFields("Norman Reilly Raine", Role.ADAPTED_BY),
                new CrewMemberTestFields("Seton I. Miller", Role.ADAPTED_BY),
                new CrewMemberTestFields("Erich Wolfgang Korngold", Role.COMPOSER),
                new CrewMemberTestFields("Ralph Dawson", Role.EDITOR),
                new CrewMemberTestFields("Errol Flynn", Role.ACTOR),
                new CrewMemberTestFields("Olivia de Havilland", Role.ACTOR),
                new CrewMemberTestFields("Basil Rathbone", Role.ACTOR),
                new CrewMemberTestFields("Claude Rains", Role.ACTOR)
        ));
        
        assertReleaseDatesEqual(flim.getReleaseDates(), ImmutableSet.of(
                new ReleaseDate(new LocalDate(1938, 5, 14), Countries.US, ReleaseType.GENERAL)
        ));
    }
}

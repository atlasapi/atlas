package org.atlasapi.remotesite.wikipedia;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.io.Resources;
import org.apache.commons.io.IOUtils;
import org.atlasapi.media.entity.Organisation;
import org.atlasapi.remotesite.wikipedia.football.FootballTeamsExtractor;
import org.atlasapi.remotesite.wikipedia.wikiparsers.Article;
import org.joda.time.DateTime;
import static org.hamcrest.Matchers.is;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class FootballTeamsExtractionTest {
    FootballTeamsExtractor extractor;

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
    public void setUp() throws IOException {
        extractor = new FootballTeamsExtractor();
    }

    @Test
    public void testArsenal() throws IOException {
        Organisation teams = extractor.extract(fakeArticle(
                IOUtils.toString(Resources.getResource(getClass(), "teams/Arsenal F.C..mediawiki").openStream(), Charsets.UTF_8.name())
        ));
        assertEquals("Arsenal", teams.getTitle());
        assertEquals("http://upload.wikimedia.org/wikipedia/en/5/53/Arsenal_FC.svg",
                Iterables.getOnlyElement(teams.getImages()).getCanonicalUri());
        assertEquals("wikipedia.org", teams.getPublisher().key());
        assertEquals("http://en.wikipedia.org/wiki/Fake_title", teams.getCanonicalUri());
        assertTrue(!teams.getRelatedLinks().isEmpty());
        Set<String> titles = ImmutableSet.of("The Gunners");
        assertThat(teams.getAlternativeTitles(), is(titles));
    }

    @Test
    public void testChelsea() throws IOException {
        Organisation teams = extractor.extract(fakeArticle(
                IOUtils.toString(Resources.getResource(getClass(), "teams/Chelsea F.C..mediawiki").openStream(), Charsets.UTF_8.name())
        ));
        assertEquals("Chelsea", teams.getTitle());
        assertEquals("http://upload.wikimedia.org/wikipedia/en/c/cc/Chelsea_FC.svg", Iterables.getOnlyElement(teams.getImages()).getCanonicalUri());
        assertEquals("wikipedia.org", teams.getPublisher().key());
        assertEquals("http://en.wikipedia.org/wiki/Fake_title", teams.getCanonicalUri());
        assertTrue(!teams.getRelatedLinks().isEmpty());
        System.out.println(teams.getAlternativeTitles());
        Set<String> titles = ImmutableSet.of("The Blues", "The Pensioners");
        assertThat(teams.getAlternativeTitles(), is(titles));
    }
}

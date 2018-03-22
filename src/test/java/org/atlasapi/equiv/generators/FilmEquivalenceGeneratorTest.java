package org.atlasapi.equiv.generators;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

import java.util.Map;

import com.metabroadcast.applications.client.model.internal.Application;
import junit.framework.TestCase;

import org.atlasapi.equiv.results.description.DefaultDescription;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResults;
import org.atlasapi.media.entity.Film;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.SearchResolver;
import org.atlasapi.search.model.SearchQuery;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.integration.junit4.JMock;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

@RunWith(JMock.class)
public class FilmEquivalenceGeneratorTest extends TestCase {

    private final Mockery context = new Mockery();
    private final SearchResolver resolver = context.mock(SearchResolver.class);
    private final Application application = mock(Application.class);
    private final FilmEquivalenceGenerator generator = new FilmEquivalenceGenerator(resolver, ImmutableSet.of(Publisher.PREVIEW_NETWORKS), application, false);
    
    private final Film subjectFilm = aFilm(10L,
            Publisher.PA, "test film title", 2000, "http://imdb.com/title/tt0409345");

    @Test
    public void testFilmWithSameTitleAndYearWithScores1() {
        checkScore(aFilm(20L, Publisher.PREVIEW_NETWORKS, "Test Film Title", 2000), Score.valueOf(1.0));
    }
    
    @Test
    public void testFilmWithImdbMatchScores1NoMatterTitleAndYear() {
        checkScore(aFilm(30L,
                Publisher.PREVIEW_NETWORKS, "Wrong Title", 2010, "http://imdb.com/title/tt0409345"), Score.valueOf(1.0));
    }

    @Test
    public void testFilmWithSameTitleButNotTolerableDifferenceYearScoresMinusOne() {
        checkScore(aFilm(40L,
                Publisher.PREVIEW_NETWORKS, "Test Film Title", 2002, "http://imdb.com/title/wrong"), Score.valueOf(-1.0));
    }

    @Test
    public void testFilmWithDifferentTitleSameYearScores0() {
        checkScore(aFilm(50L,
                Publisher.PREVIEW_NETWORKS, "Another Film Title", 2000, "http://imdb.com/title/wrong"), Score.valueOf(0.0));
    }

    private void checkScore(final Film anotherFilm, Score score) {
        context.checking(new Expectations(){{
            oneOf(resolver).search(with(searchQueryFor(subjectFilm.getTitle())), with(any(Application.class)));
                will(returnValue(ImmutableList.<Identified> of(anotherFilm)));
        }});
        
        ScoredCandidates<Item> scoredEquivalents = generator.generate(
                subjectFilm,
                new DefaultDescription(),
                EquivToTelescopeResults.create("id", "publisher")
        );
        Map<Item, Score> equivalentsScores = scoredEquivalents.candidates();
        assertThat(equivalentsScores.get(anotherFilm), is(equalTo(score)));
    }

    private Film aFilm(Long id, Publisher publisher, String title, int year,
            String... aliases) {
        Film film = new Film(title+" Uri", title+" Curie", publisher);
        film.setYear(year);
        film.setTitle(title);
        film.setId(id);
        // TODO new alias
        film.setAliasUrls(ImmutableSet.copyOf(aliases));
        return film;
    }
    
    private Matcher<SearchQuery> searchQueryFor(final String term) {
        return new TypeSafeMatcher<SearchQuery>() {

            @Override
            public void describeTo(Description desc) {
                desc.appendText("search query with term " + term);
            }

            @Override
            public boolean matchesSafely(SearchQuery query) {
                return query.getTerm().equals(term);
            }
        };
    }
    
}

package org.atlasapi.equiv.generators;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import java.lang.reflect.Field;
import java.util.Map;

import junit.framework.TestCase;

import org.atlasapi.application.v3.ApplicationConfiguration;
import org.atlasapi.equiv.EquivModule;
import org.atlasapi.equiv.results.description.DefaultDescription;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.media.entity.Film;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.SearchResolver;
import org.atlasapi.search.model.SearchQuery;

import com.metabroadcast.common.url.Urls;

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
    private final FilmEquivalenceGenerator generator = new FilmEquivalenceGenerator(resolver, ImmutableSet.of(Publisher.PREVIEW_NETWORKS), false);
    
    private final Film subjectFilm = aFilm(Publisher.PA, "test film title", 2000, "http://imdb.com/title/tt0409345");

    @Test
    public void testFilmWithSameTitleAndYearWithScores1() {
        checkScore(aFilm(Publisher.PREVIEW_NETWORKS, "Test Film Title", 2000), Score.valueOf(1.0));
    }

    @Test
    public void extractCall() {
        // this was a bad idea
        EquivModule equivModule = new EquivModule();

        Field f = null;
        try {
            f = equivModule.getClass().getDeclaredField("searchResolver");
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        }
        f.setAccessible(true);
        SearchResolver searchResolver = null; //IllegalAccessException
        try {
            searchResolver = (SearchResolver) f.get(equivModule);
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

        FilmEquivalenceGenerator filmEquivalenceGenerator = new FilmEquivalenceGenerator(
                searchResolver,
                Publisher.all(),
                false
        );

        SearchQuery query = filmEquivalenceGenerator.searchQueryFor("Zootropolis");
        String queryString = Urls.appendParameters("http://localhost:8181" + "/titles", query.toQueryStringParameters());
        System.out.println(queryString);
    }

    @Test
    public void testZootropolisMatches() {
        String title = "Zootropolis";
        Publisher publisher = Publisher.RADIO_TIMES;
        Publisher publisher2 = Publisher.YOUVIEW;
        String title2 = "Zootropolis";
        Film anotherFilm = new Film(title+" Uri", title+" Curie", publisher);
        anotherFilm.setYear(2016);
        anotherFilm.setTitle("Zootropolis");
        Score score = Score.valueOf(1.0);
        Film film = new Film(title2+" Uri", title2+" Curie", publisher2);
        film.setYear(2016);
        film.setTitle("Zootropolis");
        context.checking(new Expectations(){{
            oneOf(resolver).search(with(searchQueryFor(film.getTitle())), with(any(ApplicationConfiguration.class)));
            will(returnValue(ImmutableList.<Identified> of(anotherFilm)));
        }});

        ScoredCandidates<Item> scoredEquivalents = generator.generate(film , new DefaultDescription());
        Map<Item, Score> equivalentsScores = scoredEquivalents.candidates();
        assertThat(equivalentsScores.get(anotherFilm), is(equalTo(score)));
    }
    
    @Test
    public void testFilmWithImdbMatchScores1NoMatterTitleAndYear() {
        checkScore(aFilm(Publisher.PREVIEW_NETWORKS, "Wrong Title", 2010, "http://imdb.com/title/tt0409345"), Score.valueOf(1.0));
    }

    @Test
    public void testFilmWithSameTitleButNotTolerableDifferenceYearScoresMinusOne() {
        checkScore(aFilm(Publisher.PREVIEW_NETWORKS, "Test Film Title", 2002, "http://imdb.com/title/wrong"), Score.valueOf(-1.0));
    }

    @Test
    public void testFilmWithDifferentTitleSameYearScores0() {
        checkScore(aFilm(Publisher.PREVIEW_NETWORKS, "Another Film Title", 2000, "http://imdb.com/title/wrong"), Score.valueOf(0.0));
    }

    private void checkScore(final Film anotherFilm, Score score) {
        context.checking(new Expectations(){{
            oneOf(resolver).search(with(searchQueryFor(subjectFilm.getTitle())), with(any(ApplicationConfiguration.class)));
                will(returnValue(ImmutableList.<Identified> of(anotherFilm)));
        }});
        
        ScoredCandidates<Item> scoredEquivalents = generator.generate(subjectFilm , new DefaultDescription());
        Map<Item, Score> equivalentsScores = scoredEquivalents.candidates();
        assertThat(equivalentsScores.get(anotherFilm), is(equalTo(score)));
    }

    private Film aFilm(Publisher publisher, String title, int year, String... aliases) {
        Film film = new Film(title+" Uri", title+" Curie", publisher);
        film.setYear(year);
        film.setTitle(title);
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

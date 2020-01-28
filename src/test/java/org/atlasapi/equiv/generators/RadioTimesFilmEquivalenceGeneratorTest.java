package org.atlasapi.equiv.generators;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.base.Maybe;
import org.atlasapi.equiv.results.description.DefaultDescription;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResult;
import org.atlasapi.media.entity.Film;
import org.atlasapi.media.entity.Item;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ResolvedContent;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RadioTimesFilmEquivalenceGeneratorTest {

    @Mock
    private ContentResolver contentResolver;
    private RadioTimesFilmEquivalenceGenerator rtFilmEquivalenceGenerator;

    @Before
    public void setUp() throws Exception {
        Film film = new Film();
        film.setCanonicalUri("http://pressassociation.com/films/1");
        ResolvedContent resolvedContent = new ResolvedContent(ImmutableMap.of("http://pressassociation.com/films/1", Maybe.just(film)));
        when(contentResolver.findByUris(ImmutableSet.of("http://pressassociation.com/films/1"))).thenReturn(resolvedContent);
        when(contentResolver.findByUris(ImmutableSet.of("http://pressassociation.com/films/2"))).thenReturn(ResolvedContent.builder().build());
        this.rtFilmEquivalenceGenerator = new RadioTimesFilmEquivalenceGenerator(contentResolver, Score.valueOf(3D));
    }

    @Test
    public void testReturnsFilm() {
        Film film = new Film();
        film.setCanonicalUri("http://radiotimes.com/films/1");
        ScoredCandidates<Item> scoredCandidates = rtFilmEquivalenceGenerator.generate(
                film,
                new DefaultDescription(),
                EquivToTelescopeResult.create("id", "publisher")
        );
        Item onlyElement = Iterables.getOnlyElement(scoredCandidates.candidates().keySet());
        assertTrue(onlyElement instanceof Film);
        assertThat(onlyElement.getCanonicalUri(), is("http://pressassociation.com/films/1"));
    }

    @Test
    public void testNotReturnsFilm() {
        Film film = new Film();
        film.setCanonicalUri("http://radiotimes.com/films/2");
        ScoredCandidates<Item> scoredCandidates = rtFilmEquivalenceGenerator.generate(
                film,
                new DefaultDescription(),
                EquivToTelescopeResult.create("id", "publisher")
        );
        assertTrue(scoredCandidates.candidates().isEmpty());
    }

}
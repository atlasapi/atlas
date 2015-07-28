package org.atlasapi.equiv.generators;

import static com.google.common.collect.Iterables.filter;

import java.util.List;
import java.util.regex.Pattern;

import org.atlasapi.application.v3.ApplicationConfiguration;
import org.atlasapi.application.v3.SourceStatus;
import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates.Builder;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.media.entity.Film;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.SearchResolver;
import org.atlasapi.search.model.SearchQuery;

import com.google.common.base.Functions;
import com.google.common.base.Objects;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.query.Selection;

public class FilmEquivalenceGenerator implements EquivalenceGenerator<Item> {
    
    private static final Pattern IMDB_REF = Pattern.compile("http://imdb.com/title/[\\d\\w]+");

    private static final float TITLE_WEIGHTING = 1.0f;
    private static final float BROADCAST_WEIGHTING = 0.0f;
    private static final float CATCHUP_WEIGHTING = 0.0f;

    private final SearchResolver searchResolver;
    private final ApplicationConfiguration searchConfig;
    private final FilmTitleMatcher titleMatcher;
    private final boolean acceptNullYears;

    private final List<Publisher> publishers;


    public FilmEquivalenceGenerator(SearchResolver searchResolver, Iterable<Publisher> publishers, 
            boolean acceptNullYears) {
        this.searchResolver = searchResolver;
        this.searchConfig = defaultConfigWithSourcesEnabled(publishers);
        this.publishers = ImmutableList.copyOf(publishers);
        this.acceptNullYears = acceptNullYears;
        this.titleMatcher = new FilmTitleMatcher();
    }

    private ApplicationConfiguration defaultConfigWithSourcesEnabled(Iterable<Publisher> publishers) {
        return ApplicationConfiguration.defaultConfiguration()
                .withSources(Maps.toMap(publishers, Functions.constant(SourceStatus.AVAILABLE_ENABLED)));
    }

    @Override
    public ScoredCandidates<Item> generate(Item item, ResultDescription desc) {
        Builder<Item> scores = DefaultScoredCandidates.fromSource("Film");

        if (!(item instanceof Film)) {
            return scores.build();
        }
        
        Film film = (Film) item;
        
        if (film.getYear() == null && !acceptNullYears) {
            desc.appendText("Can't continue: null year");
            return scores.build();
        }
        
        if (Strings.isNullOrEmpty(film.getTitle())) {
            desc.appendText("Can't continue: title '%s'", film.getTitle()).finishStage();
            return scores.build();
        } else {
            desc.appendText("Using year %s, title %s", film.getYear(), film.getTitle());
        }

        Maybe<String> imdbRef = getImdbRef(film);
        if (imdbRef.hasValue()) {
            desc.appendText("Using IMDB ref %s", imdbRef.requireValue());
        }

        List<Identified> possibleEquivalentFilms = searchResolver.search(searchQueryFor(film), searchConfig);

        Iterable<Film> foundFilms = filter(possibleEquivalentFilms, Film.class);
        desc.appendText("Found %s films through title search", Iterables.size(foundFilms));

        for (Film equivFilm : foundFilms) {
            
            Maybe<String> equivImdbRef = getImdbRef(equivFilm);
            if(imdbRef.hasValue() && equivImdbRef.hasValue() && Objects.equal(imdbRef.requireValue(), equivImdbRef.requireValue())) {
                desc.appendText("%s (%s) scored 1.0 (IMDB match)", equivFilm.getTitle(), equivFilm.getCanonicalUri());
                scores.addEquivalent(equivFilm, Score.valueOf(1.0));
                
            } else if ((film.getYear() != null && sameYear(film, equivFilm)) || (film.getYear() == null && acceptNullYears)) { 
                Score score = Score.valueOf(titleMatcher.titleMatch(film, equivFilm));
                desc.appendText("%s (%s) scored %s", equivFilm.getTitle(), equivFilm.getCanonicalUri(), score);
                scores.addEquivalent(equivFilm, score);
            } else {
                desc.appendText("%s (%s) ignored. Wrong year %s", equivFilm.getTitle(), equivFilm.getCanonicalUri(), equivFilm.getYear());
                scores.addEquivalent(equivFilm, Score.negativeOne());
            }
        }
        
        return scores.build();
    }

    private Maybe<String> getImdbRef(Film film) {
     // TODO new alias
        for (String alias : film.getAliasUrls()) {
            if(IMDB_REF.matcher(alias).matches()) {
                return Maybe.just(alias);
            }
        }
        return Maybe.nothing();
    }

    private SearchQuery searchQueryFor(Film film) {
        return new SearchQuery(film.getTitle(), Selection.ALL, publishers, TITLE_WEIGHTING,
                BROADCAST_WEIGHTING, CATCHUP_WEIGHTING);
    }

    private boolean sameYear(Film film, Film equivFilm) {
        return film.getYear().equals(equivFilm.getYear());
    }
    
    @Override
    public String toString() {
        return "Film generator";
    }
}

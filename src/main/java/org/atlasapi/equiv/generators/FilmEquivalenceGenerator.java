package org.atlasapi.equiv.generators;

import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import org.atlasapi.application.v3.ApplicationConfiguration;
import org.atlasapi.application.v3.SourceStatus;
import org.atlasapi.equiv.generators.metadata.EquivalenceGeneratorMetadata;
import org.atlasapi.equiv.generators.metadata.SourceLimitedEquivalenceGeneratorMetadata;
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

import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.query.Selection;

import com.google.common.base.Functions;
import com.google.common.base.Objects;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

import static com.google.common.collect.Iterables.filter;

public class FilmEquivalenceGenerator implements EquivalenceGenerator<Item> {
    
    private static final Pattern IMDB_REF = Pattern.compile("http://imdb.com/title/[\\d\\w]+");
    private static final int NUMBER_OF_YEARS_DIFFERENT_TOLERANCE = 1;

    private static final float TITLE_WEIGHTING = 1.0f;
    private static final float BROADCAST_WEIGHTING = 0.0f;
    private static final float CATCHUP_WEIGHTING = 0.0f;

    private final Set<String> TO_REMOVE = ImmutableSet.of("rated", "unrated", "(rated)", "(unrated)");

    private final SearchResolver searchResolver;
    private final ApplicationConfiguration searchConfig;
    private final FilmTitleMatcher titleMatcher;
    private final boolean acceptNullYears;

    private final ExpandingTitleTransformer titleExpander = new ExpandingTitleTransformer();

    private final List<Publisher> publishers;


    public FilmEquivalenceGenerator(SearchResolver searchResolver, Iterable<Publisher> publishers,
            boolean acceptNullYears) {
        this.searchResolver = searchResolver;
        this.searchConfig = defaultConfigWithSourcesEnabled(publishers);
        this.publishers = ImmutableList.copyOf(publishers);
        this.acceptNullYears = acceptNullYears;
        this.titleMatcher = new FilmTitleMatcher(titleExpander);
    }

    private ApplicationConfiguration defaultConfigWithSourcesEnabled(Iterable<Publisher> publishers) {
        return ApplicationConfiguration.defaultConfiguration()
                .withSources(Maps.toMap(publishers, Functions.constant(SourceStatus.AVAILABLE_ENABLED)));
    }

    @Override
    public ScoredCandidates<Item> generate(Item item, ResultDescription desc) {
        Builder<Item> scores = DefaultScoredCandidates.fromSource("Film");

        if (!(item instanceof Film)
                || !item.isActivelyPublished()) {
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

        String title = film.getTitle();
        title = normalize(title);
        String expandedTitle = titleExpander.expand(title);

        Iterable<Identified> possibleEquivalentFilms = searchResolver.search(searchQueryFor(title), searchConfig);

        if (!title.toLowerCase().equals(expandedTitle)) {
            List<Identified> expandedTitleResults = searchResolver.search(
                    searchQueryFor(expandedTitle),
                    searchConfig
            );
            possibleEquivalentFilms = Iterables.concat(possibleEquivalentFilms,
                    expandedTitleResults
            );
        }

        Iterable<Film> foundFilms = filter(possibleEquivalentFilms, Film.class);
        desc.appendText("Found %s films through title search", Iterables.size(foundFilms));

        for (Film equivFilm : ImmutableSet.copyOf(foundFilms)) {
            
            Maybe<String> equivImdbRef = getImdbRef(equivFilm);
            if(imdbRef.hasValue() && equivImdbRef.hasValue() && Objects.equal(imdbRef.requireValue(), equivImdbRef.requireValue())) {
                desc.appendText("%s (%s) scored 1.0 (IMDB match)", equivFilm.getTitle(), equivFilm.getCanonicalUri());
                scores.addEquivalent(equivFilm, Score.valueOf(1.0));
                
            } else if ((film.getYear() != null && tolerableYearDifference(film, equivFilm)) || (film.getYear() == null && acceptNullYears)) {
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

    @Override
    public EquivalenceGeneratorMetadata getMetadata() {
        return SourceLimitedEquivalenceGeneratorMetadata.create(
                this.getClass().getCanonicalName(),
                publishers
        );
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

    private SearchQuery searchQueryFor(String title) {
        return  new SearchQuery(title, Selection.ALL, publishers, TITLE_WEIGHTING,
                BROADCAST_WEIGHTING, CATCHUP_WEIGHTING);
    }

    private boolean tolerableYearDifference(Film film, Film equivFilm) {
        if (equivFilm.getYear() == null && !acceptNullYears) {
            return false;
        } else if (equivFilm.getYear() == null && acceptNullYears) {
            return true;
        }
        return Math.abs(film.getYear() - equivFilm.getYear()) <= NUMBER_OF_YEARS_DIFFERENT_TOLERANCE;
    }

    private String normalize(String title) {
        for (String removed : TO_REMOVE) {
            title = title.toLowerCase().replaceAll(removed, "");
        }
        return title;
    }
    
    @Override
    public String toString() {
        return "Film generator";
    }
}

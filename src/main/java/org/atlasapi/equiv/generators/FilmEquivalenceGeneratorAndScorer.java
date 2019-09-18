package org.atlasapi.equiv.generators;

import com.google.common.base.Objects;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.metabroadcast.applications.client.model.internal.Application;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.query.Selection;
import org.atlasapi.equiv.generators.metadata.EquivalenceGeneratorMetadata;
import org.atlasapi.equiv.generators.metadata.SourceLimitedEquivalenceGeneratorMetadata;
import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates.Builder;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeComponent;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResult;
import org.atlasapi.media.entity.Film;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.SearchResolver;
import org.atlasapi.search.model.SearchQuery;

import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.filter;

public class FilmEquivalenceGeneratorAndScorer implements EquivalenceGenerator<Item> {
    
    private static final Pattern IMDB_REF = Pattern.compile("http://imdb.com/title/[\\d\\w]+");

    private static final float TITLE_WEIGHTING = 1.0f;
    private static final float BROADCAST_WEIGHTING = 0.0f;
    private static final float CATCHUP_WEIGHTING = 0.0f;
    private static final int DEFAULT_TOLERABLE_YEAR_DIFFERENCE = 1;
    private static final Score DEFAULT_SCORE_ON_IMDB_MATCH = Score.ONE;
    private static final Score DEFAULT_SCORE_ON_YEAR_MISMATCH = Score.negativeOne();
    private static final Score DEFAULT_SCORE_ON_TITLE_MATCH = Score.ONE;
    private static final Score DEFAULT_SCORE_ON_TITLE_MISMATCH = Score.ZERO;

    private final Set<String> TO_REMOVE = ImmutableSet.of("rated", "unrated", "(rated)", "(unrated)");

    private final SearchResolver searchResolver;
    private final Application searchApplication;
    private final FilmTitleMatcher titleMatcher;
    private final boolean acceptNullYears;

    //this var is used to ensure films with multiple release dates equiv (eg. film released across the span of two years)
    //useful if we want to constrict equiv (eg. Amazon should not have different release years)
    //Context: we had a support issue caused by a bad equiv on exact title match (for RT)
    private int tolerableYearDifference;

    private final ExpandingTitleTransformer titleExpander = new ExpandingTitleTransformer();

    private final List<Publisher> publishers;

    private Score scoreOnImdbMatch;
    private Score scoreOnYearMismatch;


    public FilmEquivalenceGeneratorAndScorer(
            SearchResolver searchResolver,
            Iterable<Publisher> publishers,
            Application application,
            boolean acceptNullYears
    ) {
        this(
                searchResolver,
                publishers,
                application,
                acceptNullYears,
                DEFAULT_TOLERABLE_YEAR_DIFFERENCE,
                DEFAULT_SCORE_ON_IMDB_MATCH,
                DEFAULT_SCORE_ON_YEAR_MISMATCH,
                DEFAULT_SCORE_ON_TITLE_MATCH,
                DEFAULT_SCORE_ON_TITLE_MISMATCH
        );
    }

    /**
     * This constructor allows you to set each of the individual score values that are used by both the generator
     * and its internal FilmTitleMatcher. It also allows you to specify a tolerance for how much the years can differ by
     * to still be considered a match.
     * @param acceptNullYears if true will match if the subject on candidate year is null
     * @param tolerableYearDifference the maximum amount the subject and candidate years can differ by to still match
     * @param scoreOnImdbMatch the score given on an IMDB match
     * @param scoreOnYearMismatch the score given if there is no IMDB match and the years do not match
     * @param scoreOnTitleMatch the score given if there is no IMDB match and the year and title match
     * @param scoreOnTitleMismatch the score given if there is no IMDB match and the year matches but the title does not
     */
    public FilmEquivalenceGeneratorAndScorer(
            SearchResolver searchResolver,
            Iterable<Publisher> publishers,
            Application application,
            boolean acceptNullYears,
            int tolerableYearDifference,
            Score scoreOnImdbMatch,
            Score scoreOnYearMismatch,
            Score scoreOnTitleMatch,
            Score scoreOnTitleMismatch
    ) {
        this.searchResolver = searchResolver;
        this.publishers = ImmutableList.copyOf(publishers);
        this.searchApplication = application;
        this.acceptNullYears = acceptNullYears;
        this.tolerableYearDifference = tolerableYearDifference;
        this.scoreOnImdbMatch = checkNotNull(scoreOnImdbMatch);
        this.scoreOnYearMismatch = checkNotNull(scoreOnYearMismatch);
        this.titleMatcher = new FilmTitleMatcher(titleExpander, scoreOnTitleMatch, scoreOnTitleMismatch);
    }

    @Override
    public ScoredCandidates<Item> generate(
            Item item,
            ResultDescription desc,
            EquivToTelescopeResult equivToTelescopeResult
    ) {
        Builder<Item> scores = DefaultScoredCandidates.fromSource("Film");

        EquivToTelescopeComponent generatorComponent = EquivToTelescopeComponent.create();
        generatorComponent.setComponentName("Film Equivalence Generator");

        if (!(item instanceof Film) || !item.isActivelyPublished()) {
            desc.appendText("Won't generate: subject is not a film");
            return scores.build();
        }
        
        Film film = (Film) item;
        
        if (!acceptNullYears && film.getYear() == null ) {
            desc.appendText("Can't generate: null year");
            equivToTelescopeResult.addGeneratorResult(generatorComponent);
            return scores.build();
        }
        
        if (Strings.isNullOrEmpty(film.getTitle())) {
            desc.appendText("Can't generate: title '%s'", film.getTitle()).finishStage();
            equivToTelescopeResult.addGeneratorResult(generatorComponent);
            return scores.build();
        } else {
            desc.appendText("Using year %s, title %s", film.getYear(), film.getTitle());
        }

        Maybe<String> imdbRef = getImdbRef(film);
        if (imdbRef.hasValue()) {
            desc.appendText("Using IMDB ref %s", imdbRef.requireValue());
        }

        String title = normalize(film.getTitle());
        String expandedTitle = titleExpander.expand(title);

        Iterable<Identified> possibleEquivalentFilms = searchResolver.search(searchQueryFor(title),
                searchApplication);

        if (!title.toLowerCase().equals(expandedTitle)) {
            List<Identified> expandedTitleResults = searchResolver.search(
                    searchQueryFor(expandedTitle),
                    searchApplication
            );
            possibleEquivalentFilms = Iterables.concat(possibleEquivalentFilms,
                    expandedTitleResults
            );
        }

        Iterable<Film> foundFilms = filter(possibleEquivalentFilms, Film.class);
        desc.appendText("Found %s films through title search", Iterables.size(foundFilms));

        for (Film equivFilm : ImmutableSet.copyOf(foundFilms)) {
            //if the candidate film is the subject itself, ignore
            if(java.util.Objects.equals(equivFilm.getId(), item.getId())){
                continue;
            }

            Maybe<String> equivImdbRef = getImdbRef(equivFilm);
            if(imdbRef.hasValue() && equivImdbRef.hasValue() && Objects.equal(imdbRef.requireValue(), equivImdbRef.requireValue())) {
                desc.appendText("%s (%s) scored %s (IMDB match)", equivFilm.getTitle(), equivFilm.getCanonicalUri(), scoreOnImdbMatch);
                scores.addEquivalent(equivFilm, scoreOnImdbMatch);

                if (equivFilm.getId() != null) {
                    generatorComponent.addComponentResult(
                            equivFilm.getId(),
                            scoreOnImdbMatch.toString()
                    );
                }
                
            } else if ((film.getYear() != null && tolerableYearDifference(film, equivFilm)) || (film.getYear() == null && acceptNullYears)) {
                Score score = titleMatcher.titleMatch(film, equivFilm);
                desc.appendText("%s (%s) scored %s", equivFilm.getTitle(), equivFilm.getCanonicalUri(), score);
                scores.addEquivalent(equivFilm, score);

                if (equivFilm.getId() != null) {
                    generatorComponent.addComponentResult(
                            equivFilm.getId(),
                            score.toString()
                    );
                }
            } else {
                scores.addEquivalent(equivFilm, scoreOnYearMismatch);

                if (equivFilm.getId() != null) {
                    desc.appendText("%s (%s) ignored. Wrong year %s", equivFilm.getTitle(), equivFilm.getCanonicalUri(), equivFilm.getYear());
                    generatorComponent.addComponentResult(
                            equivFilm.getId(),
                            scoreOnYearMismatch.toString()
                    );
                }
            }
        }

        equivToTelescopeResult.addGeneratorResult(generatorComponent);
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
        return Math.abs(film.getYear() - equivFilm.getYear()) <= tolerableYearDifference;
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

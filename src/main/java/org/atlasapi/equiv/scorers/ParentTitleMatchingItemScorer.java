package org.atlasapi.equiv.scorers;

import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

import org.atlasapi.equiv.generators.ExpandingTitleTransformer;
import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates.Builder;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeComponent;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResults;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Item;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ResolvedContent;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.StringUtils;

/**
 * This scorer will look for the parents of each candidate, and will give the candidate the full
 * score for each parent that matches exactly. No partial scores are given. Series and Brands are
 * scored separately. If the main item does not have a parent everything gets ZERO for that parent.
 * if the main item has a parent, but the candidate does not, he will get the missmatch score.
 */
public class ParentTitleMatchingItemScorer implements EquivalenceScorer<Item> {

    public String name ;
    private Score DEFAULT_SCORE = Score.ZERO;

    public enum TitleType {

        DATE("\\d{1,2}/\\d{1,2}/(\\d{2}|\\d{4})"),
        SEQUENCE("((?:E|e)pisode)(?:.*)(\\d+)"),
        DEFAULT(".*");

        private Pattern pattern;

        TitleType(String pattern) {
            this.pattern = Pattern.compile(pattern);
        }

        public static TitleType titleTypeOf(Item item) {
            return titleTypeOf(item.getTitle());
        }

        public static TitleType titleTypeOf(String title) {
            for (TitleType type : ImmutableList.copyOf(TitleType.values())) {
                if (type.matches(title)) {
                    return type;
                }
            }
            return DEFAULT;
        }

        private boolean matches(String title) {
            return pattern.matcher(title).matches();
        }

    }

    private ContentResolver contentResolver;
    private final Score scoreOnSeriesMatch;
    private final Score scoreOnBrandMatch;
    private final Score scoreOnSeriesMismatch;
    private final Score scoreOnBrandMismatch;

    public ParentTitleMatchingItemScorer(
            ContentResolver contentResolver,
            Score scoreOnSeriesMatch,
            Score scoreOnBrandMatch,
            Score scoreOnSeriesMismatch,
            Score scoreOnBrandMismatch) {
        this.contentResolver = contentResolver;
        this.scoreOnSeriesMatch = scoreOnSeriesMatch;
        this.scoreOnBrandMatch = scoreOnBrandMatch;
        this.scoreOnSeriesMismatch = scoreOnSeriesMismatch;
        this.scoreOnBrandMismatch = scoreOnBrandMismatch;

        this.name = String.format("Parent Title Scorer (Brand match=%s, Brand mismatch=%s, Series match=%s, Series mismatch=%s)",
                scoreOnBrandMatch,
                scoreOnBrandMismatch,
                scoreOnSeriesMatch,
                scoreOnSeriesMismatch);
    }

    @Override
    public ScoredCandidates<Item> score(
            Item subject,
            Set<? extends Item> suggestions,
            ResultDescription desc,
            EquivToTelescopeResults equivToTelescopeResults
    ) {
        EquivToTelescopeComponent scorerComponent = EquivToTelescopeComponent.create();
        scorerComponent.setComponentName(name);
        Builder<Item> equivalents = DefaultScoredCandidates.fromSource(name);

        Container series = getSeries(subject);
        Container brand = getBrand(subject);

        if(series == null){
            desc.appendText("No Series found. Everything will get a score of %s for Series.", DEFAULT_SCORE);
        }
        if(brand == null){
            desc.appendText("No Brand found. Everything will get a score of %s for Brands.", DEFAULT_SCORE);
        }

        for (Item suggestion : suggestions) {
            Score equivScore = score(brand, series, suggestion, desc);
            equivalents.addEquivalent(suggestion, equivScore);
            scorerComponent.addComponentResult(
                    suggestion.getId(),
                    String.valueOf(equivScore.asDouble())
            );
        }

        equivToTelescopeResults.addScorerResult(scorerComponent);

        return equivalents.build();
    }

    @Nullable
    private Container getBrand(Item item) {

        if (item.getContainer() != null) {
            ResolvedContent resolvedBrand =
                    contentResolver.findByCanonicalUris(
                            ImmutableSet.of(item.getContainer().getUri()));
            if (resolvedBrand.getFirstValue().hasValue()) {
                return (Container) resolvedBrand.getFirstValue().requireValue();
            }
        }

        return null;
    }

    @Nullable
    private Container getSeries(Item item) {
        if (item instanceof Episode) {
            Episode episode = (Episode) item;
            if (episode.getSeriesRef() != null) {
                ResolvedContent resolvedSeries =
                        contentResolver.findByCanonicalUris(ImmutableSet.of(episode.getSeriesRef()
                                .getUri()));
                if (resolvedSeries.getFirstValue().hasValue()) {
                    return (Container) resolvedSeries.getFirstValue().requireValue();
                }
            }
        }
        return null;
    }

    private Score score(@Nullable Container brand, @Nullable Container series,
            Item suggestion, ResultDescription desc) {

        Container suggBrand = getBrand(suggestion);
        Container suggSeries = getSeries(suggestion);
        Score brandScore = DEFAULT_SCORE;
        Score seriesScore = DEFAULT_SCORE;

        if (series != null ) {
            if(suggSeries != null){
                if(series.getTitle().equalsIgnoreCase(suggSeries.getTitle())) {
                    desc.appendText("Series title match for %s Scored: %s", suggestion.getCanonicalUri(), scoreOnSeriesMatch);
                    seriesScore = scoreOnSeriesMatch;
                } else {
                    desc.appendText("Series title mismatch for %s Scored: %s", suggestion.getCanonicalUri(), scoreOnSeriesMismatch);
                    seriesScore = scoreOnSeriesMismatch;
                }
            } else {
                desc.appendText("No Series found. Title mismatch for %s Scored: %s", suggestion.getCanonicalUri(), scoreOnSeriesMismatch);
                seriesScore = scoreOnSeriesMismatch;
            }
        }

        if (brand != null ) {
            if( suggBrand != null) {
                if (brand.getTitle().equalsIgnoreCase(suggBrand.getTitle())) {
                    desc.appendText("Brand title match for %s Scored: %s", suggestion.getCanonicalUri(), scoreOnBrandMatch);
                    brandScore = scoreOnBrandMatch;
                } else {
                    desc.appendText("Brand title mismatch for %s Scored: %s", suggestion.getCanonicalUri(), scoreOnBrandMismatch);
                    brandScore = scoreOnBrandMismatch;
                }
            } else {
                desc.appendText("No Brand found. Title mismatch for %s Scored: %s", suggestion.getCanonicalUri(), scoreOnBrandMismatch);
                brandScore = scoreOnBrandMismatch;
            }
        }

        return brandScore.add(seriesScore);
    }

    @Override
    public String toString() {
        return name;
    }
}

package org.atlasapi.equiv.scorers;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Ordering;
import com.google.common.collect.PeekingIterator;
import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates.Builder;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeComponent;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResult;
import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Series;
import org.atlasapi.media.entity.SeriesRef;
import org.atlasapi.persistence.content.ContentResolver;

import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

public class ContainerHierarchyMatchingScorer implements EquivalenceScorer<Container> {

    private static final int MAX_EPISODE_DIFFERENCE = 1;
    private static final int MAX_SERIES_DIFFERENCE = 1;
    private static final int SERIES_DIFFERENCE_TO_GIVE_MISMATCH_SCORE = 3;

    private final ContentResolver contentResolver;
    private final Score mismatchScore;
    private final SubscriptionCatchupBrandDetector subscriptionCatchupBrandDetector;

    public ContainerHierarchyMatchingScorer(ContentResolver contentResolver, Score mismatchScore, 
            SubscriptionCatchupBrandDetector subscriptionCatchupBrandDetector) {
        this.contentResolver = contentResolver;
        this.mismatchScore = mismatchScore;
        this.subscriptionCatchupBrandDetector = checkNotNull(subscriptionCatchupBrandDetector);
    }
    
    @Override
    public ScoredCandidates<Container> score(
            Container subj,
            Set<? extends Container> candidates,
            ResultDescription desc,
            EquivToTelescopeResult equivToTelescopeResult
    ) {
        EquivToTelescopeComponent scorerComponent = EquivToTelescopeComponent.create();
        scorerComponent.setComponentName("Container Hierarchy Matching Scorer");

        Builder<Container> results = DefaultScoredCandidates.fromSource("Hierarchy");

        // Brands can have full Series hierarchy so compare its Series' hierarchies if present. 
        // If there are no Series treat it as a flat Container
        if (subj instanceof Brand && !((Brand)subj).getSeriesRefs().isEmpty()) {
            ImmutableList<Series> series = seriesFor((Brand)subj);
            if (subscriptionCatchupBrandDetector.couldBeSubscriptionCatchup(subj, series)) {
                for (Container cand : candidates) {
                    results.addEquivalent(cand, Score.nullScore());
                    scorerComponent.addComponentResult(
                            cand.getId(),
                            ""
                    );
                }
                desc.appendText("Detected as subscription catchup brand, not scoring hierarchies");
                desc.finishStage();
                return results.build();
            }
            List<Integer> subjSeriesSizes = sortedSeriesSizes(series);
            desc.appendText("Subject %s, %s series: %s", subj, subjSeriesSizes.size(), subjSeriesSizes)
                .startStage("matches:");
            for (Container cand : candidates) {
                Score equivScore = score(subjSeriesSizes, cand, desc);
                results.addEquivalent(cand, equivScore);

                if (cand.getId() != null) {
                    scorerComponent.addComponentResult(
                            cand.getId(),
                            String.valueOf(equivScore.asDouble())
                    );
                }

            }
        } else {
            desc.appendText("Subject %s, no series:", subj)
                .startStage("matches:");
            if (subj.getChildRefs().isEmpty()) {
                desc.appendText("Subject has no episodes: all score null");
                for (Container candidate : candidates) {
                    results.addEquivalent(candidate, Score.nullScore());

                    if (candidate.getId() != null) {
                        scorerComponent.addComponentResult(
                                candidate.getId(),
                                ""
                        );
                    }
                }
            } else {
                for (Container candidate : candidates) {
                    Score equivScore = score(subj, candidate, desc);
                    results.addEquivalent(candidate, equivScore);

                    if (candidate.getId() != null) {
                        scorerComponent.addComponentResult(
                                candidate.getId(),
                                String.valueOf(equivScore.asDouble())
                        );
                    }
                }
            }
        }

        equivToTelescopeResult.addScorerResult(scorerComponent);
        
        desc.finishStage();
        return results.build();
    }

    private Score score(List<Integer> subjSeriesSizes, Container cand, ResultDescription desc) {
        if (!(cand instanceof Brand)) {
            desc.appendText("%s: not Brand -> %.2f", cand, mismatchScore.asDouble());
            return mismatchScore;
        }
        return score(subjSeriesSizes, (Brand)cand, desc);
    }
    
    private Score score(List<Integer> subjSeriesSizes, Brand cand, ResultDescription desc) {
        
        if (cand.getSeriesRefs().isEmpty()) {
            desc.appendText("%s: series count 0 -> %.2f", cand, mismatchScore.asDouble());
            return mismatchScore;
        }
        
        return scoreSortedSeriesSizes(subjSeriesSizes, sortedSeriesSizes(seriesFor(cand)), cand.getCanonicalUri(), desc);
    }

    @VisibleForTesting //TODO: extract into helper
    public Score scoreSortedSeriesSizes(List<Integer> subjSeriesSizes, List<Integer> candSeriesSizes, String candUri, ResultDescription desc) {
        PeekingIterator<Integer> subjAllocation = Iterators.peekingIterator(subjSeriesSizes.iterator());
        PeekingIterator<Integer> candAllocation = Iterators.peekingIterator(candSeriesSizes.iterator());
       
        int droppedCount = 0;
        int subj = 0;
        int cand = 0;
        
        int subjExhausted = 0;
        int candExhausted = 0;
        
        while(subjAllocation.hasNext() && candAllocation.hasNext()) {
            
            subj = subjAllocation.next();
            cand = candAllocation.next();
            
            subjExhausted++;
            candExhausted++;
            
            if (!acceptable(subj, cand)) {
                droppedCount++;
                if (candAllocation.hasNext() && acceptable(subj, candAllocation.peek())) {
                    cand = candAllocation.next();
                    candExhausted++;
                } else if (subjAllocation.hasNext() && acceptable(subjAllocation.peek(), cand)) {
                    subj = subjAllocation.next();
                    subjExhausted++;
                }
            }
        }

        int subjNotExhausted = subjSeriesSizes.size() - subjExhausted;
        int candNotExhausted = candSeriesSizes.size() - candExhausted;
        int seriesMismatches = Math.max(subjNotExhausted, candNotExhausted) + droppedCount;
        if (seriesMismatches <= MAX_SERIES_DIFFERENCE) {
            desc.appendText("%s: series episode counts %s -> 1", candUri, subjSeriesSizes, candSeriesSizes);
            return Score.ONE;
        }
        
        if (seriesMismatches >= SERIES_DIFFERENCE_TO_GIVE_MISMATCH_SCORE) {
            desc.appendText("%s: series episode counts %s -> %.2f", candUri, candSeriesSizes, mismatchScore.asDouble());
            return mismatchScore;
        }
        
        desc.appendText("%s: series episode counts %s -> none", candUri, candSeriesSizes);
        return Score.nullScore();
        
    }
    
    private boolean acceptable(int sub, int sug) {
        return Math.abs(sub - sug) <= MAX_EPISODE_DIFFERENCE;
    }

    private ImmutableList<Integer> sortedSeriesSizes(List<Series> subjectSeries) {
        return Ordering.natural().immutableSortedCopy(Iterables.transform(subjectSeries, new Function<Series, Integer>() {
            @Override
            public Integer apply(Series input) {
                return input.getChildRefs().size();
            }
        }));
    }

    //Simple case were container hierarchy is flat: compare episode counts.
    private Score score(Container subj, Container cand, ResultDescription desc) {
        if (cand instanceof Brand && !((Brand)cand).getSeriesRefs().isEmpty()) {
            return Score.nullScore();
        }
        
        int subjChildren = subj.getChildRefs().size();
        int candChildren = cand.getChildRefs().size();
        
        if (candChildren == 0) {
            desc.appendText("%s scores none (0 episodes)", cand);
            return Score.nullScore();
        } else if (acceptable(subjChildren, candChildren)) {
            desc.appendText("%s scores 1 (|%s-%s| <= %s)", cand, subjChildren, candChildren, MAX_EPISODE_DIFFERENCE);
            return Score.ONE;
        } else {
            desc.appendText("%s scores none (|%s-%s| > %s)", cand, subjChildren, candChildren, MAX_EPISODE_DIFFERENCE);
            return Score.nullScore();
        }
    }

    public ImmutableList<Series> seriesFor(Brand brand) {
        Iterable<String> uris = ImmutableList.copyOf(Iterables.transform(brand.getSeriesRefs(), SeriesRef.TO_URI));
        List<Identified> allResolvedSeries = contentResolver.findByCanonicalUris(uris).getAllResolvedResults();
        return ImmutableList.copyOf(Iterables.filter(allResolvedSeries, Series.class));
    }

    @Override
    public String toString() {
        return "Container Hierarchy Scorer";
    }
}

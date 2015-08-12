package org.atlasapi.equiv.results.combining;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoreThreshold;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.media.entity.Content;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Maps.EntryTransformer;

public class RequiredScoreFilteringCombiner<T extends Content> implements ScoreCombiner<T> {

    private final ScoreCombiner<T> delegate;
    private final ImmutableSet<String> sources;
    private final ScoreThreshold threshold;

    public RequiredScoreFilteringCombiner(ScoreCombiner<T> delegate, String source, ScoreThreshold threshold) {
        this(delegate, ImmutableSet.of(source), threshold);
    }
    
    public RequiredScoreFilteringCombiner(ScoreCombiner<T> delegate, Iterable<String> source, ScoreThreshold threshold) {
        this.delegate = delegate;
        this.sources = ImmutableSet.copyOf(source);
        this.threshold = threshold;
    }
    
    public RequiredScoreFilteringCombiner(ScoreCombiner<T> delegate, String source) {
        this(delegate, source, ScoreThreshold.positive());
    }
    
    public RequiredScoreFilteringCombiner(ScoreCombiner<T> delegate, Iterable<String> sources) {
        this(delegate, sources, ScoreThreshold.positive());
    }
    
    @Override
    public ScoredCandidates<T> combine(List<ScoredCandidates<T>> scoredEquivalents, final ResultDescription desc) {
        ScoredCandidates<T> combined = delegate.combine(scoredEquivalents, desc);
        
        desc.startStage("Filtering null " +  sources + " scores");
        
        Iterable<ScoredCandidates<T>> itemScores = findItemScores(scoredEquivalents);
        
        if(Iterables.isEmpty(itemScores)) {
            desc.appendText("No %s scores found", sources).finishStage();
            return combined;
        }
        
        final Multimap<T, Score> itemScoreMap = ArrayListMultimap.create();
        
        for (ScoredCandidates<T> itemScore : itemScores) {
            for (T item : itemScore.candidates().keySet()) {
                itemScoreMap.put(item, itemScore.candidates().get(item));
            }
        }
        
        Map<T, Score> transformedCombined = ImmutableMap.copyOf(Maps.transformEntries(combined.candidates(), new EntryTransformer<T, Score, Score>() {
            @Override
            public Score transformEntry(T equiv, Score combinedScore) {
                Collection<Score> itemScores = itemScoreMap.get(equiv);

                Optional<Score> found = Iterables.tryFind(itemScores, new Predicate<Score>(){

                    @Override
                    public boolean apply(Score input) {
                        return input != null 
                                && threshold.apply(input);
                    }});
                
                if (!found.isPresent()) {
                    desc.appendText("%s score set to null, %s score %s", equiv.getCanonicalUri(), sources, itemScores);
                    return Score.NULL_SCORE;
                }
                
                return combinedScore;
            }
        }));
        desc.finishStage();
        return DefaultScoredCandidates.fromMappedEquivs(combined.source(), transformedCombined);
    }
    
    private Iterable<ScoredCandidates<T>> findItemScores(List<ScoredCandidates<T>> scoredEquivalents) {
        
        return Iterables.filter(scoredEquivalents, new Predicate<ScoredCandidates<T>>() {

            @Override
            public boolean apply(ScoredCandidates<T> input) {
                return sources.contains(input.source());
            }
            
        });
    }

}

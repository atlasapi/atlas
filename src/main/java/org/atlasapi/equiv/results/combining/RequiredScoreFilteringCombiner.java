package org.atlasapi.equiv.results.combining;

import java.util.List;
import java.util.Map;

import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoreThreshold;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.media.entity.Content;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
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
        
        ScoredCandidates<T> itemScores = findItemScores(scoredEquivalents);
        
        if(itemScores == null) {
            desc.appendText("No %s scores found", sources).finishStage();
            return combined;
        }
        
        final Map<T, Score> itemScoreMap = itemScores.candidates();
        
        Map<T, Score> transformedCombined = ImmutableMap.copyOf(Maps.transformEntries(combined.candidates(), new EntryTransformer<T, Score, Score>() {
            @Override
            public Score transformEntry(T equiv, Score combinedScore) {
                Score itemScore = itemScoreMap.get(equiv);

                if (itemScore == null) {
                    return Score.NULL_SCORE;
                }
                
                if (threshold.apply(itemScore)) {
                    return combinedScore;
                }
                
                desc.appendText("%s score set to null, %s score %s", equiv.getCanonicalUri(), sources, itemScore);
                return Score.NULL_SCORE;
            }
        }));
        desc.finishStage();
        return DefaultScoredCandidates.fromMappedEquivs(combined.source(), transformedCombined);
    }
    
    private ScoredCandidates<T> findItemScores(List<ScoredCandidates<T>> scoredEquivalents) {
        for (ScoredCandidates<T> sourceEquivs : scoredEquivalents) {
            if(sources.contains(sourceEquivs.source())) {
                return sourceEquivs;
            }
        }
        return null;
    }

}

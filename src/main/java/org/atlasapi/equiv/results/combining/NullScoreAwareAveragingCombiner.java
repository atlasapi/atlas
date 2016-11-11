package org.atlasapi.equiv.results.combining;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.media.entity.Described;
import org.atlasapi.media.entity.Publisher;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Maps.EntryTransformer;

public class NullScoreAwareAveragingCombiner<T extends Described> implements ScoreCombiner<T> {
    
    private final boolean ignoreNullScoringCandidate;

    public static final <T extends Described> NullScoreAwareAveragingCombiner<T> get() {
        return new NullScoreAwareAveragingCombiner<T>();
    }

    public NullScoreAwareAveragingCombiner() {
        this.ignoreNullScoringCandidate = false;
    }
    
    public NullScoreAwareAveragingCombiner(boolean ignoreNullScoringCandidate) {
        this.ignoreNullScoringCandidate = ignoreNullScoringCandidate;
    }
    
    @Override
    public ScoredCandidates<T> combine(
            List<ScoredCandidates<T>> scoredEquivalents,
            ResultDescription desc
    ) {
        
        desc.startStage("Null-score-aware combining");
        
        Map<T,Score> tempResults = Maps.newHashMap();
        final Map<T, Integer> counts = Maps.newHashMap();
        List<String> source = Lists.newArrayListWithCapacity(tempResults.size());
        
        // For each equivalent, count the sources that produced a non-null Score 
        // and total those Scores.
        for (ScoredCandidates<T> sourceEquivalents : scoredEquivalents) {
            source.add(sourceEquivalents.source());
            
            for (Entry<T, Score> equivScore : sourceEquivalents.candidates().entrySet()) {
                
                addCount(counts, equivScore);
                addScore(tempResults, equivScore);
            }
                
        }
        
        // For each publisher, find the maximum number of non-null scores for
        // Content from that Publisher
        final Map<Publisher, Integer> publisherCounts = transformToPublisherCounts(counts);
        
        // Average the scores by the publisher counts.
        Map<T, Score> scaledScores = scaleResultsByCounts(tempResults, publisherCounts);
        
        desc.finishStage();
        return DefaultScoredCandidates.fromMappedEquivs(Joiner.on("/").join(source), scaledScores);
    }

    private void addScore(Map<T, Score> tempResults, Entry<T, Score> equivScore) {
        Score curRes = tempResults.get(equivScore.getKey());
        tempResults.put(equivScore.getKey(), curRes == null ?
                                             equivScore.getValue() :
                                             curRes.add(equivScore.getValue()));
    }

    private void addCount(final Map<T, Integer> counts, Entry<T, Score> equivScore) {
        if(equivScore.getValue().isRealScore()) {
            Integer curCount = counts.get(equivScore.getKey());
            counts.put(equivScore.getKey(), curCount == null ? 1 : curCount+1);
        }
    }

    private Map<T, Score> scaleResultsByCounts(
            Map<T, Score> tempResults,
            final Map<Publisher, Integer> publisherCounts
    ) {
        return Maps.transformEntries(tempResults, new EntryTransformer<T, Score, Score>() {
            @Override
            public Score transformEntry(T key, Score value) {
                if (value.isRealScore()) {
                    Integer count = publisherCounts.get(key.getPublisher());

                    // if count == 1 then the only score came from the generator
                    // which we sometimes want to not equivalate just because of
                    if (ignoreNullScoringCandidate && count == 1) {
                        return Score.ZERO;
                    }

                    return Score.valueOf(value.asDouble() / (count != null ? count : 1));
                } else {
                    return value;
                }
            }
        });
    }

    private Map<Publisher, Integer> transformToPublisherCounts(final Map<T, Integer> counts) {
        final Map<Publisher, Integer> publisherCounts = Maps.newHashMap();
        for (Entry<T, Integer> equivalentSourceCount : counts.entrySet()) {
            Integer cur = publisherCounts.get(equivalentSourceCount.getKey().getPublisher());
            if (cur == null || cur < equivalentSourceCount.getValue()) {
                publisherCounts.put(
                        equivalentSourceCount.getKey().getPublisher(),
                        equivalentSourceCount.getValue()
                );
            }
        }
        return publisherCounts;
    }

}

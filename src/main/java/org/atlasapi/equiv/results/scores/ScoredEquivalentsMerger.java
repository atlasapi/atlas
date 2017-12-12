package org.atlasapi.equiv.results.scores;

import static org.atlasapi.equiv.results.scores.ScoredCandidates.TO_SOURCE;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.atlasapi.media.entity.Content;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

public class ScoredEquivalentsMerger {

    public <T extends Content> List<ScoredCandidates<T>> merge(
            List<ScoredCandidates<T>> lefts,
            List<ScoredCandidates<T>> rights
    ) {
        
        Map<String,ScoredCandidates<T>> left = Maps.uniqueIndex(lefts, TO_SOURCE);
        ImmutableMap<String, ScoredCandidates<T>> right = Maps.uniqueIndex(rights, TO_SOURCE);
        
        Map<String, ScoredCandidates<T>> merged = Maps.newHashMap();
        
        for (String source : ImmutableSet.copyOf(Iterables.concat(left.keySet(), right.keySet()))) {
            if(!left.containsKey(source)) {
                merged.put(source, right.get(source));
            } else if(!right.containsKey(source)) {
                merged.put(source, left.get(source));
            } else {
                merged.put(source, merge(left.get(source), (right.get(source))));
            }
        }
        
        return ImmutableList.copyOf(merged.values());
        
    }

    private <T extends Content> ScoredCandidates<T> merge(ScoredCandidates<T> left, ScoredCandidates<T> right) {
        HashMap<T, Score> rightMap = Maps.newHashMap(right.candidates());
        rightMap.putAll(left.candidates());
        return DefaultScoredCandidates.fromMappedEquivs(left.source(), rightMap);
    }
   
}

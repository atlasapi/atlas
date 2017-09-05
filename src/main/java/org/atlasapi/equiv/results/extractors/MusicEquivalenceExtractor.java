package org.atlasapi.equiv.results.extractors;

import java.util.List;

import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.ScoredCandidate;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeComponent;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResults;
import org.atlasapi.media.entity.Item;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;

public class MusicEquivalenceExtractor implements EquivalenceExtractor<Item> {

    private static final double SINGLE_THRESHOLD = 0.2;
    private static final double MULTI_THRESHOLD = 0.7;

    @Override
    public Optional<ScoredCandidate<Item>> extract(
            List<ScoredCandidate<Item>> candidates,
            Item subject,
            ResultDescription desc,
            EquivToTelescopeResults equivToTelescopeResults
    ) {
        EquivToTelescopeComponent extractorComponent = EquivToTelescopeComponent.create();
        extractorComponent.setComponentName("Music Equivalence Extractor");

        if (candidates.isEmpty()) {
            return Optional.absent();
        }
        
        desc.startStage(toString());
        
        List<ScoredCandidate<Item>> positiveScores = removeNonPositiveScores(candidates, desc);
        
        Optional<ScoredCandidate<Item>> result = Optional.absent();
        
        if (positiveScores.size() == 1) {
            ScoredCandidate<Item> only = positiveScores.get(0);
            result = candidateIfOverThreshold(only, SINGLE_THRESHOLD, desc);
        } else if (positiveScores.size() > 1) {
            ScoredCandidate<Item> only = positiveScores.get(0);
            result = candidateIfOverThreshold(only, MULTI_THRESHOLD, desc);
        }

        extractorComponent.addComponentResult(
                result.get().candidate().getId(),
                String.valueOf(result.get().score())
        );
        
        desc.finishStage();
        return result; 
    }

    private List<ScoredCandidate<Item>> removeNonPositiveScores(List<ScoredCandidate<Item>> candidates, ResultDescription desc) {
        List<ScoredCandidate<Item>> positiveScores = Lists.newLinkedList();
        for (ScoredCandidate<Item> candidate : candidates) {
            if (candidate.score().asDouble() > 0.0) {
                positiveScores.add(candidate);
            } else {
                desc.appendText("%s removed (non-positive score)", candidate.candidate());
            }
        }
        return positiveScores;
    }

    private Optional<ScoredCandidate<Item>> candidateIfOverThreshold(ScoredCandidate<Item> only, double threshold, ResultDescription desc) {
        if (only.score().asDouble() > threshold) {
            desc.appendText("%s beats %s threshold", only.candidate(), threshold);
            return Optional.of(only);
        } else {
            desc.appendText("%s under %s threshold", only.candidate(), threshold);
            return Optional.absent();
        }
    }

    @Override
    public String toString() {
        return "Music Filter";
    }
    
}

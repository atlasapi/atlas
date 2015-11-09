package org.atlasapi.equiv.scorers;

import java.util.Set;

import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates.Builder;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Item;

import com.google.common.base.Objects;

public class SeriesSequenceItemScorer implements EquivalenceScorer<Item> {

    private static final Score NEGATIVE_ONE = Score.valueOf(-1.0);

    @Override
    public ScoredCandidates<Item> score(Item subject, Set<? extends Item> candidates,
            ResultDescription desc) {
        Builder<Item> equivalents = DefaultScoredCandidates.fromSource("Series");
        
        if (subject instanceof Episode) {
            Episode episode = (Episode) subject;
            desc.appendText("Subject: Series: %s. %s candidates",
                episode.getSeriesNumber(),
                candidates.size()
            );
            for (Item candidate : candidates) {
                Score score = score(episode, candidate, desc);
                equivalents.addEquivalent(candidate, score);
            }
        } else {
            desc.appendText("Subject: not epsiode");
            for (Item suggestion : candidates) {
                equivalents.addEquivalent(suggestion, Score.NULL_SCORE);
            }
        }
        
        return equivalents.build();
    }

    private Score score(Episode subject, Item candidate, ResultDescription desc) {
        
        if (!(candidate instanceof Episode)) {
            desc.appendText("%s not episode", candidate);
            return Score.nullScore();
        }

        Episode candidateEpisode = (Episode) candidate;
        
        Integer subjSeriesNumber = subject.getSeriesNumber();
        Integer candSeriesNumber = candidateEpisode.getSeriesNumber();
        
        Score score;
        if (subjSeriesNumber == null) {
            score = Score.nullScore();
        } else if (subjSeriesNumber == null && candSeriesNumber == null) {
            score = Score.nullScore();
        } else if (Objects.equal(subjSeriesNumber, candSeriesNumber)) {
            score = Score.ONE;
        } else {
            score = NEGATIVE_ONE;
        }
        
        desc.appendText("%s: series number %s, %s", 
                candidate, candSeriesNumber, score);
        
        return score;
    }

    @Override
    public String toString() {
        return "Series Sequence";
    }
}

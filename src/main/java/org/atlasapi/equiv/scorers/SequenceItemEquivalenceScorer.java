package org.atlasapi.equiv.scorers;

import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates.Builder;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Item;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

public class SequenceItemEquivalenceScorer implements EquivalenceScorer<Item> {

    @Override
    public ScoredCandidates<Item> score(Item subject, Iterable<Item> suggestions, ResultDescription desc) {
        Builder<Item> equivalents = DefaultScoredCandidates.fromSource("Sequence");

        desc.appendText("%s suggestions", Iterables.size(suggestions));

        for (Item suggestion : Iterables.filter(ImmutableSet.copyOf(suggestions), Item.class)) {
            equivalents.addEquivalent(suggestion, score(subject, suggestion, desc));
        }

        return equivalents.build();
    }

    private Score score(Item subject, Item suggestion, ResultDescription desc) {

        if (subject instanceof Episode && suggestion instanceof Episode) {

            Episode subEp = (Episode) subject;
            Episode sugEp = (Episode) suggestion;

            if (Objects.equal(subEp.getSeriesNumber(), sugEp.getSeriesNumber()) && subEp.getEpisodeNumber() != null && sugEp.getEpisodeNumber() != null
                    && Objects.equal(subEp.getEpisodeNumber(), sugEp.getEpisodeNumber())) {
                Score score = Score.valueOf(1.0);
                desc.appendText("%s (%s) S: %s, E: %s scored %s", sugEp.getTitle(), sugEp.getCanonicalUri(), sugEp.getSeriesNumber(), sugEp.getEpisodeNumber(), score);
                return score;
            }
        }

        return Score.NULL_SCORE;
    }

    @Override
    public String toString() {
        return "Sequence Item Scorer";
    }
}

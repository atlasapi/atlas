package org.atlasapi.equiv.scorers;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Set;

import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.Item;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

public class BroadcastAliasScorer implements EquivalenceScorer<Item> {

    private static final String NAME = "Broadcast-Alias";
    private static final Function<Broadcast, Iterable<String>> BROADCAST_TO_ALIAS_STRING_ITERABLE = new Function<Broadcast, Iterable<String>>() {

        @Override
        public Iterable<String> apply(Broadcast input) {
            return input.getAliasUrls();
        }

    };
    
    private static final Function<Broadcast, Iterable<Alias>> BROADCAST_TO_ALIAS_ITERABLE = new Function<Broadcast, Iterable<Alias>>() {

        @Override
        public Iterable<Alias> apply(Broadcast input) {
            return input.getAliases();
        }

    };
    
    private final Score mismatchScore;

    public BroadcastAliasScorer(Score mismatchScore) {
        this.mismatchScore = checkNotNull(mismatchScore);
    }

    @Override public ScoredCandidates<Item> score(Item subject, Set<? extends Item> candidates,
            ResultDescription desc) {
        DefaultScoredCandidates.Builder<Item> equivalents = DefaultScoredCandidates.fromSource(NAME);

        for (Item candidate : candidates) {
            equivalents.addEquivalent(candidate, score(subject, candidate));
        }

        return equivalents.build();
    }

    private Score score(Item subject, Item candidate) {
        ImmutableSet<String> aliasStringsOfCandidateBroadcasts = FluentIterable.from(candidate.flattenBroadcasts())
                .transformAndConcat(BROADCAST_TO_ALIAS_STRING_ITERABLE)
                .toSet();

        ImmutableSet<String> aliasStringsOfSubjectBroadcasts = FluentIterable.from(subject.flattenBroadcasts())
                .transformAndConcat(BROADCAST_TO_ALIAS_STRING_ITERABLE)
                .toSet();

        if (!Sets.intersection(aliasStringsOfCandidateBroadcasts, aliasStringsOfSubjectBroadcasts)
                .isEmpty()) {
            return Score.ONE;
        }
        
        ImmutableSet<Alias> aliasesOfCandidateBroadcasts = FluentIterable.from(candidate.flattenBroadcasts())
                .transformAndConcat(BROADCAST_TO_ALIAS_ITERABLE)
                .toSet();

        ImmutableSet<Alias> aliasesOfSubjectBroadcasts = FluentIterable.from(subject.flattenBroadcasts())
                .transformAndConcat(BROADCAST_TO_ALIAS_ITERABLE)
                .toSet();
        
        if (!Sets.intersection(aliasesOfCandidateBroadcasts, aliasesOfSubjectBroadcasts)
                .isEmpty()) {
            return Score.ONE;
        }
        
        return mismatchScore;
    }
}

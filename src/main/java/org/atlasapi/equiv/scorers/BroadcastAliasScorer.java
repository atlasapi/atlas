package org.atlasapi.equiv.scorers;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.metabroadcast.common.stream.MoreCollectors;
import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeComponent;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResults;
import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Item;

import java.util.Set;
import java.util.stream.StreamSupport;

import static com.google.common.base.Preconditions.checkNotNull;

public class BroadcastAliasScorer implements EquivalenceScorer<Item> {

    private static final String NAME = "Broadcast-Alias";

    private final Score mismatchScore;

    public BroadcastAliasScorer(Score mismatchScore) {
        this.mismatchScore = checkNotNull(mismatchScore);
    }

    @Override public ScoredCandidates<Item> score(
            Item subject,
            Set<? extends Item> candidates,
            ResultDescription desc,
            EquivToTelescopeResults equivToTelescopeResults
    ) {
        EquivToTelescopeComponent scorerComponent = EquivToTelescopeComponent.create();
        scorerComponent.setComponentName("Broadcast Alias Scorer");

        DefaultScoredCandidates.Builder<Item> equivalents = DefaultScoredCandidates.fromSource(NAME);

        for (Item candidate : candidates) {
            Score equivScore = score(subject, candidate);
            equivalents.addEquivalent(candidate, equivScore);

            scorerComponent.addComponentResult(
                    candidate.getId(),
                    String.valueOf(equivScore.asDouble())
            );
        }

        equivToTelescopeResults.addScorerResult(scorerComponent);

        return equivalents.build();
    }

    private Score score(Item subject, Item candidate) {
        ImmutableSet<String> aliasStringsOfCandidateBroadcasts = StreamSupport.stream(candidate.flattenBroadcasts().spliterator(), false)
                .flatMap(iden -> iden.getAliasUrls().stream())
                .collect(MoreCollectors.toImmutableSet());

        ImmutableSet<String> aliasStringsOfSubjectBroadcasts = StreamSupport.stream(subject.flattenBroadcasts().spliterator(), false)
                .flatMap(iden -> iden.getAliasUrls().stream())
                .collect(MoreCollectors.toImmutableSet());

        if (!Sets.intersection(aliasStringsOfCandidateBroadcasts, aliasStringsOfSubjectBroadcasts)
                .isEmpty()) {
            return Score.ONE;
        }

        ImmutableSet<Alias> aliasesOfCandidateBroadcasts = StreamSupport.stream(candidate.flattenBroadcasts().spliterator(), false)
                .flatMap(iden -> iden.getAliases().stream())
                .collect(MoreCollectors.toImmutableSet());

        ImmutableSet<Alias> aliasesOfSubjectBroadcasts = StreamSupport.stream(subject.flattenBroadcasts().spliterator(), false)
                .flatMap(iden -> iden.getAliases().stream())
                .collect(MoreCollectors.toImmutableSet());
        
        if (!Sets.intersection(aliasesOfCandidateBroadcasts, aliasesOfSubjectBroadcasts)
                .isEmpty()) {
            return Score.ONE;
        }
        
        return mismatchScore;
    }
}

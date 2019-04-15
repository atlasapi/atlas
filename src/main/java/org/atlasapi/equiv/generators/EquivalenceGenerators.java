package org.atlasapi.equiv.generators;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.stream.MoreCollectors;
import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResult;
import org.atlasapi.media.entity.Content;

import java.util.List;
import java.util.Set;

public class EquivalenceGenerators<T extends Content> {

    private final List<? extends EquivalenceGenerator<T>> generators;
    private final Set<String> excludedUris;
    private final Set<String> excludedIds;
    private final SubstitutionTableNumberCodec codec;

    private EquivalenceGenerators(
            Iterable<? extends EquivalenceGenerator<T>> generators,
            Set<String> excludedUris,
            Set<String> excludedIds
    ) {
        this.generators = ImmutableList.copyOf(generators);
        this.excludedUris = excludedUris;
        this.excludedIds = excludedIds;
        this.codec = SubstitutionTableNumberCodec.lowerCaseOnly();
    }

    public static <T extends Content> EquivalenceGenerators<T> create(
            Iterable<? extends EquivalenceGenerator<T>> generators,
            Set<String> excludedUris,
            Set<String> excludedIds
    ) {
        return new EquivalenceGenerators<T>(generators, excludedUris, excludedIds);
    }

    public List<ScoredCandidates<T>> generate(
            T content,
            ResultDescription desc,
            EquivToTelescopeResult equivToTelescopeResult
    ) {
        desc.startStage("Generating equivalences");
        Builder<ScoredCandidates<T>> generatedScores = ImmutableList.builder();

        ImmutableList<Long> excludedDecodedIds = excludedIds.stream()
                .map(id -> codec.decode(id).longValue())
                .collect(MoreCollectors.toImmutableList());

        if (excludedUris.contains(content.getCanonicalUri())
                || excludedDecodedIds.contains(content.getId())) {
            desc.appendText("Content %s is in equivalence blacklist and will not be equivalated",
                    content.getCanonicalUri());
            return generatedScores.build();
        }

        if (!content.isActivelyPublished()) {
            return generatedScores.build();
        }

        for (EquivalenceGenerator<T> generator : generators) {
            try {
                desc.startStage(generator.toString());
                generatedScores.add(generator.generate(content, desc, equivToTelescopeResult));
                desc.finishStage();
            } catch (Exception e) {
                throw new RuntimeException(String.format("Exception running %s for %s", generator, content), e);
            }
        }
        
        desc.finishStage();
        return generatedScores.build();
    }
    
    @Override
    public String toString() {
        return Objects.toStringHelper(getClass())
                .add("generators", generators)
                .toString();
    }
}

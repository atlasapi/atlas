package org.atlasapi.equiv.update;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.stream.MoreCollectors;
import org.atlasapi.equiv.generators.EquivalenceGenerator;
import org.atlasapi.equiv.generators.EquivalenceGenerators;
import org.atlasapi.equiv.results.DefaultEquivalenceResultBuilder;
import org.atlasapi.equiv.results.EquivalenceResult;
import org.atlasapi.equiv.results.combining.ScoreCombiner;
import org.atlasapi.equiv.results.description.ReadableDescription;
import org.atlasapi.equiv.results.extractors.EquivalenceExtractor;
import org.atlasapi.equiv.results.filters.EquivalenceFilter;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.equiv.results.scores.ScoredEquivalentsMerger;
import org.atlasapi.equiv.scorers.EquivalenceScorer;
import org.atlasapi.equiv.scorers.EquivalenceScorers;
import org.atlasapi.equiv.update.metadata.ContentEquivalenceResultProviderMetadata;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResult;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResults;
import org.atlasapi.equiv.update.metadata.EquivalenceUpdaterMetadata;
import org.atlasapi.media.entity.Content;

import java.math.BigInteger;
import java.util.List;
import java.util.Set;
import java.util.stream.StreamSupport;

public class ContentEquivalenceResultUpdater<T extends Content> implements EquivalenceResultUpdater<T> {

    private final ScoredEquivalentsMerger merger;

    private final EquivalenceGenerators<T> generators;
    private final EquivalenceScorers<T> scorers;
    private final DefaultEquivalenceResultBuilder<T> resultBuilder;

    private final SubstitutionTableNumberCodec codec
            = SubstitutionTableNumberCodec.lowerCaseOnly();

    private final EquivalenceUpdaterMetadata metadata;

    private ContentEquivalenceResultUpdater(Builder<T> builder) {
        ImmutableSet<EquivalenceGenerator<T>> builtGenerators = builder.generators.build();
        ImmutableSet<EquivalenceScorer<T>> builtScorers = builder.scorers.build();
        ImmutableList<EquivalenceExtractor<T>> builtExtractors = builder.extractors.build();

        this.merger = new ScoredEquivalentsMerger();
        this.generators = EquivalenceGenerators.create(
                builtGenerators,
                builder.excludedUris,
                builder.excludedIds
        );
        this.scorers = EquivalenceScorers.from(builtScorers);
        this.resultBuilder = new DefaultEquivalenceResultBuilder<>(
                builder.combiner,
                builder.filter,
                builtExtractors
        );

        this.metadata = ContentEquivalenceResultProviderMetadata.builder()
                .withGenerators(builtGenerators)
                .withScorers(builtScorers)
                .withCombiner(builder.combiner)
                .withFilter(builder.filter)
                .withExtractors(builtExtractors)
                .withExcludedUris(builder.excludedUris)
                .withExcludedIds(builder.excludedIds)
                .build();
    }

    public static <T extends Content> ExcludedUrisStep<T> builder() {
        return new Builder<>();
    }

    @Override
    public EquivalenceResult<T> provideEquivalenceResult(
            T content,
            ReadableDescription desc,
            EquivToTelescopeResults resultsForTelescope
    ) {
        EquivToTelescopeResult resultForTelescope = EquivToTelescopeResult.create(
                codec.encode(BigInteger.valueOf(content.getId())),
                content.getPublisher().toString()
        );

        List<ScoredCandidates<T>> generatedScores = generators.generate(
                content,
                desc,
                resultForTelescope
        );

        Set<T> candidates = ImmutableSet.copyOf(extractCandidates(generatedScores));
        
        List<ScoredCandidates<T>> scoredScores = scorers.score(
                content,
                candidates,
                desc,
                resultForTelescope
        );
        
        List<ScoredCandidates<T>> mergedScores = merger.merge(
                generatedScores,
                scoredScores
        );
        
        EquivalenceResult<T> result = resultBuilder.resultFor(
                content,
                mergedScores,
                desc,
                resultForTelescope
        );

        resultsForTelescope.addResult(resultForTelescope);

        return result;
    }

    @Override
    public EquivalenceUpdaterMetadata getMetadata() {
        return metadata;
    }

    private Iterable<T> extractCandidates(Iterable<ScoredCandidates<T>> generatedScores) {

        return StreamSupport.stream(generatedScores.spliterator(), false)
                .map(input -> input.candidates().keySet())
                .flatMap(Set::stream)
                .collect(MoreCollectors.toImmutableSet());
    }

    public interface ExcludedUrisStep<T extends Content> {

        ExcludedIdsStep<T> withExcludedUris(Set<String> excludedUris);
    }

    public interface ExcludedIdsStep<T extends Content> {

        GeneratorStep<T> withExcludedIds(Set<String> excludedIds);
    }

    public interface GeneratorStep<T extends Content> {

        ScorerStep<T> withGenerator(EquivalenceGenerator<T> generator);

        ScorerStep<T> withGenerators(Iterable<? extends EquivalenceGenerator<T>> generators);
    }

    public interface ScorerStep<T extends Content> {

        CombinerStep<T> withScorer(EquivalenceScorer<T> scorer);

        CombinerStep<T> withScorers(Iterable<? extends EquivalenceScorer<T>> scorers);
    }

    public interface CombinerStep<T extends Content> {

        FilterStep<T> withCombiner(ScoreCombiner<T> combiner);
    }

    public interface FilterStep<T extends Content> {

        ExtractorStep<T> withFilter(EquivalenceFilter<T> filter);
    }

    public interface ExtractorStep<T extends Content> {

        BuildStep<T> withExtractor(EquivalenceExtractor<T> extractor);

        BuildStep<T> withExtractors(List<EquivalenceExtractor<T>> extractors);
    }

    public interface BuildStep<T extends Content> {

        ContentEquivalenceResultUpdater<T> build();
    }

    public static class Builder<T extends Content> implements ExcludedUrisStep<T>,
            ExcludedIdsStep<T>, GeneratorStep<T>, ScorerStep<T>, CombinerStep<T>, FilterStep<T>,
            ExtractorStep<T>, BuildStep<T> {

        private ImmutableSet.Builder<EquivalenceGenerator<T>> generators = ImmutableSet.builder();
        private ImmutableSet.Builder<EquivalenceScorer<T>> scorers = ImmutableSet.builder();
        private ScoreCombiner<T> combiner;
        private EquivalenceFilter<T> filter;
        private ImmutableList.Builder<EquivalenceExtractor<T>> extractors = ImmutableList.builder();
        private Set<String> excludedUris;
        private Set<String> excludedIds;

        private Builder() {
        }

        @Override
        public ExcludedIdsStep<T> withExcludedUris(Set<String> excludedUris) {
            this.excludedUris = excludedUris;
            return this;
        }

        @Override
        public GeneratorStep<T> withExcludedIds(Set<String> excludedIds) {
            this.excludedIds = excludedIds;
            return this;
        }

        @Override
        public ScorerStep<T> withGenerator(EquivalenceGenerator<T> generator) {
            generators.add(generator);
            return this;
        }

        @Override
        public ScorerStep<T> withGenerators(
                Iterable<? extends EquivalenceGenerator<T>> generators
        ) {
            this.generators.addAll(generators);
            return this;
        }

        @Override
        public CombinerStep<T> withScorer(EquivalenceScorer<T> scorer) {
            this.scorers.add(scorer);
            return this;
        }

        @Override
        public CombinerStep<T> withScorers(Iterable<? extends EquivalenceScorer<T>> scorers) {
            this.scorers.addAll(scorers);
            return this;
        }

        @Override
        public FilterStep<T> withCombiner(ScoreCombiner<T> combiner) {
            this.combiner = combiner;
            return this;
        }

        @Override
        public ExtractorStep<T> withFilter(EquivalenceFilter<T> filter) {
            this.filter = filter;
            return this;
        }

        @Override
        public BuildStep<T> withExtractor(EquivalenceExtractor<T> extractor) {
            this.extractors.add(extractor);
            return this;
        }

        @Override
        public BuildStep<T> withExtractors(List<EquivalenceExtractor<T>> extractors) {
            this.extractors.addAll(extractors);
            return this;
        }

        public ContentEquivalenceResultUpdater<T> build() {
            return new ContentEquivalenceResultUpdater<>(this);
        }
    }
}

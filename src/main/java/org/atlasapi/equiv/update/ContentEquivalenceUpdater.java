package org.atlasapi.equiv.update;

import java.util.List;
import java.util.Set;

import org.atlasapi.equiv.generators.EquivalenceGenerator;
import org.atlasapi.equiv.generators.EquivalenceGenerators;
import org.atlasapi.equiv.handlers.EquivalenceResultHandler;
import org.atlasapi.equiv.messengers.EquivalenceResultMessenger;
import org.atlasapi.equiv.results.DefaultEquivalenceResultBuilder;
import org.atlasapi.equiv.results.EquivalenceResult;
import org.atlasapi.equiv.results.combining.ScoreCombiner;
import org.atlasapi.equiv.results.description.DefaultDescription;
import org.atlasapi.equiv.results.description.ReadableDescription;
import org.atlasapi.equiv.results.extractors.EquivalenceExtractor;
import org.atlasapi.equiv.results.filters.EquivalenceFilter;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.equiv.results.scores.ScoredEquivalentsMerger;
import org.atlasapi.equiv.scorers.EquivalenceScorer;
import org.atlasapi.equiv.scorers.EquivalenceScorers;
import org.atlasapi.equiv.update.metadata.ContentEquivalenceUpdaterMetadata;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResults;
import org.atlasapi.equiv.update.metadata.EquivalenceUpdaterMetadata;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.reporting.telescope.OwlTelescopeReporter;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.gson.Gson;
import com.google.gson.JsonElement;

import static com.google.common.base.Preconditions.checkNotNull;

public class ContentEquivalenceUpdater<T extends Content> implements EquivalenceUpdater<T> {

    private final ScoredEquivalentsMerger merger;

    private final EquivalenceGenerators<T> generators;
    private final EquivalenceScorers<T> scorers;
    private final DefaultEquivalenceResultBuilder<T> resultBuilder;
    private final EquivalenceResultHandler<T> handler;
    private final EquivalenceResultMessenger<T> messenger;

    private final EquivalenceUpdaterMetadata metadata;

    private ContentEquivalenceUpdater(Builder<T> builder) {
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
        this.handler = checkNotNull(builder.handler);
        this.messenger = checkNotNull(builder.messenger);

        this.metadata = ContentEquivalenceUpdaterMetadata.builder()
                .withGenerators(builtGenerators)
                .withScorers(builtScorers)
                .withCombiner(builder.combiner)
                .withFilter(builder.filter)
                .withExtractors(builtExtractors)
                .withHandler(handler)
                .withExcludedUris(builder.excludedUris)
                .withExcludedIds(builder.excludedIds)
                .build();
    }

    public static <T extends Content> ExcludedUrisStep<T> builder() {
        return new Builder<>();
    }

    @Override
    public boolean updateEquivalences(T content, OwlTelescopeReporter telescope) {
        ReadableDescription desc = new DefaultDescription();

        EquivToTelescopeResults resultsForTelescope = EquivToTelescopeResults.create(
                String.valueOf(content.getId()),
                content.getPublisher().toString()
        );

        List<ScoredCandidates<T>> generatedScores = generators.generate(
                content,
                desc,
                resultsForTelescope
        );
        
        Set<T> candidates = ImmutableSet.copyOf(extractCandidates(generatedScores));
        
        List<ScoredCandidates<T>> scoredScores = scorers.score(
                content,
                candidates,
                desc,
                resultsForTelescope
        );
        
        List<ScoredCandidates<T>> mergedScores = merger.merge(
                generatedScores,
                scoredScores
        );
        
        EquivalenceResult<T> result = resultBuilder.resultFor(
                content,
                mergedScores,
                desc,
                resultsForTelescope
        );

        boolean handledWithStateChange = handler.handle(result);

        if (handledWithStateChange) {
            messenger.sendMessage(result);
        }

        Gson gson = new Gson();
        JsonElement equivResultsJson = gson.toJsonTree(resultsForTelescope);

        telescope.reportSuccessfulEvent(
                content.getId(),
                content.getAliases(),
                content,
                equivResultsJson
        );

        return !result.combinedEquivalences().candidates().isEmpty();
    }

    @Override
    public EquivalenceUpdaterMetadata getMetadata(Set<Publisher> sources) {
        return metadata;
    }

    private Iterable<T> extractCandidates(Iterable<ScoredCandidates<T>> generatedScores) {
        return Iterables.concat(Iterables.transform(
                generatedScores,
                (Function<ScoredCandidates<T>, Iterable<T>>) input -> input.candidates().keySet()
        ));
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

        HandlerStep<T> withExtractor(EquivalenceExtractor<T> extractor);

        HandlerStep<T> withExtractors(List<EquivalenceExtractor<T>> extractors);
    }

    public interface HandlerStep<T extends Content> {

        MessengerStep<T> withHandler(EquivalenceResultHandler<T> handler);
    }

    public interface MessengerStep<T extends Content> {

        BuildStep<T> withMessenger(EquivalenceResultMessenger<T> messenger);
    }

    public interface BuildStep<T extends Content> {

        ContentEquivalenceUpdater<T> build();
    }

    public static class Builder<T extends Content> implements ExcludedUrisStep<T>,
            ExcludedIdsStep<T>, GeneratorStep<T>, ScorerStep<T>, CombinerStep<T>, FilterStep<T>,
            ExtractorStep<T>, HandlerStep<T>, MessengerStep<T>, BuildStep<T> {

        private ImmutableSet.Builder<EquivalenceGenerator<T>> generators = ImmutableSet.builder();
        private ImmutableSet.Builder<EquivalenceScorer<T>> scorers = ImmutableSet.builder();
        private ScoreCombiner<T> combiner;
        private EquivalenceFilter<T> filter;
        private ImmutableList.Builder<EquivalenceExtractor<T>> extractors;
        private EquivalenceResultHandler<T> handler;
        private EquivalenceResultMessenger<T> messenger;
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
        public HandlerStep<T> withExtractor(EquivalenceExtractor<T> extractor) {
            this.extractors.add(extractor);
            return this;
        }

        @Override
        public HandlerStep<T> withExtractors(List<EquivalenceExtractor<T>> extractors) {
            this.extractors.addAll(extractors);
            return this;
        }

        @Override
        public MessengerStep<T> withHandler(EquivalenceResultHandler<T> handler) {
            this.handler = handler;
            return this;
        }

        @Override
        public BuildStep<T> withMessenger(EquivalenceResultMessenger<T> messenger) {
            this.messenger = messenger;
            return this;
        }

        public ContentEquivalenceUpdater<T> build() {
            return new ContentEquivalenceUpdater<>(this);
        }
    }
}

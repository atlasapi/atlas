package org.atlasapi.equiv.update.metadata;

import java.util.List;
import java.util.Set;
import java.util.stream.StreamSupport;

import org.atlasapi.equiv.generators.EquivalenceGenerator;
import org.atlasapi.equiv.generators.metadata.EquivalenceGeneratorMetadata;
import org.atlasapi.equiv.handlers.EquivalenceResultHandler;
import org.atlasapi.equiv.scorers.EquivalenceScorer;

import com.metabroadcast.common.stream.MoreCollectors;

import com.google.common.collect.ImmutableList;

import static com.google.common.base.Preconditions.checkNotNull;

public class ContentEquivalenceUpdaterMetadata extends EquivalenceUpdaterMetadata {

    private final ImmutableList<EquivalenceGeneratorMetadata> generators;
    private final ImmutableList<String> scorers;
    private final String combiner;
    private final String filter;
    private final String extractor;
    private final String handler;
    private final ImmutableList<String> excludedUris;
    private final ImmutableList<String> excludedIds;

    private ContentEquivalenceUpdaterMetadata(
            Iterable<EquivalenceGeneratorMetadata> generators,
            Iterable<String> scorers,
            String combiner,
            String filter,
            String extractor,
            String handler,
            Iterable<String> excludedUris,
            Iterable<String> excludedIds
    ) {
        this.generators = ImmutableList.copyOf(generators);
        this.scorers = ImmutableList.copyOf(scorers);
        this.combiner = checkNotNull(combiner);
        this.filter = checkNotNull(filter);
        this.extractor = checkNotNull(extractor);
        this.handler = checkNotNull(handler);
        this.excludedUris = ImmutableList.copyOf(excludedUris);
        this.excludedIds = ImmutableList.copyOf(excludedIds);
    }

    public static GeneratorsStep builder() {
        return new Builder();
    }

    public ImmutableList<EquivalenceGeneratorMetadata> getGenerators() {
        return generators;
    }

    public ImmutableList<String> getScorers() {
        return scorers;
    }

    public String getCombiner() {
        return combiner;
    }

    public String getFilter() {
        return filter;
    }

    public String getExtractor() {
        return extractor;
    }

    public String getHandler() {
        return handler;
    }

    public ImmutableList<String> getExcludedUris() {
        return excludedUris;
    }

    public ImmutableList<String> getExcludedIds() {
        return excludedIds;
    }

    public interface GeneratorsStep {

        <T> ScorersStep withGenerators(Iterable<EquivalenceGenerator<T>> generators);
    }

    public interface ScorersStep {

        <T> CombinerStep withScorers(Iterable<EquivalenceScorer<T>> scorers);
    }

    public interface CombinerStep {

        FilterStep withCombiner(Object combiner);
    }

    public interface FilterStep {

        ExtractorStep withFilter(Object filter);
    }

    public interface ExtractorStep {

        HandlerStep withExtractor(Object extractor);
    }

    public interface HandlerStep {

        <T> ExcludedUrisStep withHandler(EquivalenceResultHandler<T> handler);
    }

    public interface ExcludedUrisStep {

        ExcludedIdsStep withExcludedUris(Set<String> excludedUris);
    }

    public interface ExcludedIdsStep {

        BuildStep withExcludedIds(Set<String> excludedUris);
    }

    public interface BuildStep {

        ContentEquivalenceUpdaterMetadata build();
    }

    public static class Builder implements GeneratorsStep, ScorersStep, CombinerStep, FilterStep,
            ExtractorStep, HandlerStep, ExcludedUrisStep, ExcludedIdsStep, BuildStep {

        private List<EquivalenceGeneratorMetadata> generators;
        private List<String> scorers;
        private String combiner;
        private String filter;
        private String extractor;
        private String handler;
        private Set<String> excludedUris;
        private Set<String> excludedIds;

        private Builder() {
        }

        @Override
        public <T> ScorersStep withGenerators(Iterable<EquivalenceGenerator<T>> generators) {
            this.generators = StreamSupport.stream(generators.spliterator(), false)
                    .map(EquivalenceGenerator::getMetadata)
                    .collect(MoreCollectors.toImmutableList());
            return this;
        }

        @Override
        public <T> CombinerStep withScorers(Iterable<EquivalenceScorer<T>> scorers) {
            this.scorers = getClassName(scorers);
            return this;
        }

        @Override
        public FilterStep withCombiner(Object combiner) {
            this.combiner = getClassName(combiner);
            return this;
        }

        @Override
        public ExtractorStep withFilter(Object filter) {
            this.filter = getClassName(filter);
            return this;
        }

        @Override
        public HandlerStep withExtractor(Object extractor) {
            this.extractor = getClassName(extractor);
            return this;
        }

        @Override
        public <T> ExcludedUrisStep withHandler(EquivalenceResultHandler<T> handler) {
            this.handler = getClassName(handler);
            return this;
        }

        @Override
        public ExcludedIdsStep withExcludedUris(Set<String> excludedUris) {
            this.excludedUris = excludedUris;
            return this;
        }

        @Override
        public BuildStep withExcludedIds(Set<String> excludedIds) {
            this.excludedIds = excludedIds;
            return this;
        }

        @Override
        public ContentEquivalenceUpdaterMetadata build() {
            return new ContentEquivalenceUpdaterMetadata(
                    this.generators,
                    this.scorers,
                    this.combiner,
                    this.filter,
                    this.extractor,
                    this.handler,
                    this.excludedUris,
                    this.excludedIds
            );
        }
    }
}

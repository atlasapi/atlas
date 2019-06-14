package org.atlasapi.equiv.update.metadata;

import com.google.common.collect.ImmutableList;
import com.metabroadcast.common.stream.MoreCollectors;
import org.atlasapi.equiv.generators.EquivalenceGenerator;
import org.atlasapi.equiv.generators.metadata.EquivalenceGeneratorMetadata;
import org.atlasapi.equiv.results.extractors.EquivalenceExtractor;
import org.atlasapi.equiv.scorers.EquivalenceScorer;

import java.util.List;
import java.util.Set;
import java.util.stream.StreamSupport;

import static com.google.common.base.Preconditions.checkNotNull;

public class ContentEquivalenceResultProviderMetadata extends EquivalenceUpdaterMetadata {

    private final ImmutableList<EquivalenceGeneratorMetadata> generators;
    private final ImmutableList<String> scorers;
    private final String combiner;
    private final String filter;
    private final ImmutableList<String> extractor;
    private final ImmutableList<String> excludedUris;
    private final ImmutableList<String> excludedIds;

    private ContentEquivalenceResultProviderMetadata(
            Iterable<EquivalenceGeneratorMetadata> generators,
            Iterable<String> scorers,
            String combiner,
            String filter,
            Iterable<String> extractors,
            Iterable<String> excludedUris,
            Iterable<String> excludedIds
    ) {
        this.generators = ImmutableList.copyOf(generators);
        this.scorers = ImmutableList.copyOf(scorers);
        this.combiner = checkNotNull(combiner);
        this.filter = checkNotNull(filter);
        this.extractor = ImmutableList.copyOf(extractors);
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

    public ImmutableList<String> getExtractor() {
        return extractor;
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

        <T> ExcludedUrisStep withExtractors(Iterable<EquivalenceExtractor<T>> extractors);
    }

    public interface ExcludedUrisStep {

        ExcludedIdsStep withExcludedUris(Set<String> excludedUris);
    }

    public interface ExcludedIdsStep {

        BuildStep withExcludedIds(Set<String> excludedUris);
    }

    public interface BuildStep {

        ContentEquivalenceResultProviderMetadata build();
    }

    public static class Builder implements GeneratorsStep, ScorersStep, CombinerStep, FilterStep,
            ExtractorStep, ExcludedUrisStep, ExcludedIdsStep, BuildStep {

        private List<EquivalenceGeneratorMetadata> generators;
        private List<String> scorers;
        private String combiner;
        private String filter;
        private List<String> extractors;
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
        public <T> ExcludedUrisStep withExtractors(Iterable<EquivalenceExtractor<T>> extractors) {
            this.extractors = getClassName(extractors);
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
        public ContentEquivalenceResultProviderMetadata build() {
            return new ContentEquivalenceResultProviderMetadata(
                    this.generators,
                    this.scorers,
                    this.combiner,
                    this.filter,
                    this.extractors,
                    this.excludedUris,
                    this.excludedIds
            );
        }
    }
}

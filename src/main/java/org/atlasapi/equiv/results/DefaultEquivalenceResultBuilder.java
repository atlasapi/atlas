package org.atlasapi.equiv.results;

import java.util.List;
import java.util.Set;

import org.atlasapi.equiv.results.combining.ScoreCombiner;
import org.atlasapi.equiv.results.description.ReadableDescription;
import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.extractors.EquivalenceExtractor;
import org.atlasapi.equiv.results.extractors.MultipleCandidateExtractor;
import org.atlasapi.equiv.results.filters.EquivalenceFilter;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates;
import org.atlasapi.equiv.results.scores.ScoredCandidate;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeComponent;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResults;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Publisher;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.Ordering;
import com.google.common.collect.SortedSetMultimap;
import com.google.common.collect.TreeMultimap;

public class DefaultEquivalenceResultBuilder<T extends Content>
        implements EquivalenceResultBuilder<T> {

    private final ScoreCombiner<T> combiner;
    private final EquivalenceExtractor<T> extractor;
    private final EquivalenceFilter<T> filter;

    private final MultipleCandidateExtractor<T> multipleCandidateExtractor;

    public DefaultEquivalenceResultBuilder(
            ScoreCombiner<T> combiner,
            EquivalenceFilter<T> filter,
            EquivalenceExtractor<T> extractor
    ) {
        this.combiner = combiner;
        this.filter = filter;
        this.extractor = extractor;

        this.multipleCandidateExtractor = MultipleCandidateExtractor.create();
    }

    public static <T extends Content> EquivalenceResultBuilder<T> create(
            ScoreCombiner<T> combiner,
            EquivalenceFilter<T> filter,
            EquivalenceExtractor<T> marker
    ) {
        return new DefaultEquivalenceResultBuilder<T>(combiner, filter, marker);
    }

    @Override
    public EquivalenceResult<T> resultFor(
            T target,
            List<ScoredCandidates<T>> equivalents,
            ReadableDescription desc,
            EquivToTelescopeResults equivToTelescopeResults
    ) {
        ScoredCandidates<T> combined = combine(equivalents, desc, equivToTelescopeResults);
        List<ScoredCandidate<T>> filteredCandidates = filter(
                target,
                desc,
                combined,
                equivToTelescopeResults
        );
        Multimap<Publisher, ScoredCandidate<T>> extractedScores = extract(
                target,
                filteredCandidates,
                desc,
                equivToTelescopeResults
        );
        return new EquivalenceResult<T>(target, equivalents, combined, extractedScores, desc);
    }
    
    private ScoredCandidates<T> combine(
            List<ScoredCandidates<T>> equivalents,
            ResultDescription desc,
            EquivToTelescopeResults equivToTelescopeResults
    ) {
        desc.startStage("Combining scores");
        ScoredCandidates<T> combination;
        if (!equivalents.isEmpty()) {
            combination = combiner.combine(equivalents, desc);
        } else {
            combination = DefaultScoredCandidates.<T>fromSource("empty combination").build();
        }
        desc.finishStage();
        return combination;
    }

    private List<ScoredCandidate<T>> filter(
            T target,
            ReadableDescription desc,
            ScoredCandidates<T> combined,
            EquivToTelescopeResults equivToTelescopeResults
    ) {
        desc.startStage("Filtering candidates");
        List<ScoredCandidate<T>> filteredCandidates = ImmutableList.copyOf(
            filter.apply(
                    combined.orderedCandidates(Ordering.usingToString()),
                    target,
                    desc,
                    equivToTelescopeResults
            )
        );
        desc.finishStage();
        return filteredCandidates;
    }

    private Multimap<Publisher, ScoredCandidate<T>> extract(
            T target,
            List<ScoredCandidate<T>> filteredCandidates,
            ResultDescription desc,
            EquivToTelescopeResults equivToTelescopeResults
    ) {
        desc.startStage("Extracting strong equivalents");
        SortedSetMultimap<Publisher, ScoredCandidate<T>> publisherBins =
                publisherBin(filteredCandidates);
        
        ImmutableMultimap.Builder<Publisher, ScoredCandidate<T>> builder =
                ImmutableMultimap.builder();
        
        for (Publisher publisher : publisherBins.keySet()) {
            desc.startStage(String.format("Publisher: %s", publisher));
            
            ImmutableSortedSet<ScoredCandidate<T>> copyOfSorted =
                    ImmutableSortedSet.copyOfSorted(publisherBins.get(publisher));

            Optional<Set<ScoredCandidate<T>>> multipleExtractedCandidates =
                    multipleCandidateExtractor.extract(
                            copyOfSorted.asList().reverse(),
                            target,
                            equivToTelescopeResults
                    );

            if (multipleExtractedCandidates.isPresent()) {
                builder.putAll(
                        publisher,
                        multipleExtractedCandidates.get()
                );
            } else {
                Optional<ScoredCandidate<T>> extracted = extractor.extract(
                        copyOfSorted.asList().reverse(),
                        target,
                        desc,
                        equivToTelescopeResults
                );
                if (extracted.isPresent()) {
                    builder.put(publisher, extracted.get());
                }
            }

            desc.finishStage();
        }
        
        desc.finishStage();
        return builder.build();
    }

    private SortedSetMultimap<Publisher, ScoredCandidate<T>> publisherBin(
            List<ScoredCandidate<T>> filteredCandidates
    ) {
        SortedSetMultimap<Publisher, ScoredCandidate<T>> publisherBins =
                TreeMultimap.create(
                        Ordering.natural(),
                        ScoredCandidate.SCORE_ORDERING.compound(Ordering.usingToString())
                );
        
        for (ScoredCandidate<T> candidate : filteredCandidates) {
            publisherBins.put(candidate.candidate().getPublisher(), candidate);
        }
        
        return publisherBins;
    }
}

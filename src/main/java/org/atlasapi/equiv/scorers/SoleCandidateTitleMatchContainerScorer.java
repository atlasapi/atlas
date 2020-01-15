package org.atlasapi.equiv.scorers;

import com.google.common.base.Functions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.metabroadcast.applications.client.model.internal.Application;
import com.metabroadcast.common.query.Selection;
import com.metabroadcast.common.stream.MoreCollectors;
import org.atlasapi.application.v3.DefaultApplication;
import org.atlasapi.equiv.generators.ContentTitleScorer;
import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeComponent;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResult;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Described;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.SearchResolver;
import org.atlasapi.search.model.SearchQuery;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.stream.StreamSupport.stream;

/**
 * Score a candidate if exact title match & it is the only candidate from a particular publisher.
 * Uses an inner ContentTitleScorer's logic to calculate if title matches exactly.
 * Searches for exact title + publisher as subject; scores all candidates null if any results found,
 * otherwise scores as above.
 */
public class SoleCandidateTitleMatchContainerScorer implements EquivalenceScorer<Container>{

    public static final String NAME = "Sole Candidate Title Match";

    private final ContentTitleScorer<Container> titleScorer;
    private final SearchResolver searchResolver;
    private final Score exactTitleMatchScore;

    private final Score mismatchScore = Score.nullScore();

    public SoleCandidateTitleMatchContainerScorer(
            Score exactTitleMatchScore,
            Score partialTitleMatchBound,
            SearchResolver searchResolver
    ) {
        this.titleScorer = new ContentTitleScorer<>(
                NAME,
                Functions.identity(),
                exactTitleMatchScore,
                partialTitleMatchBound
        );
        this.searchResolver = searchResolver;
        this.exactTitleMatchScore = exactTitleMatchScore;
    }

    @Override
    public ScoredCandidates<Container> score(
            Container subject,
            Set<? extends Container> candidates,
            ResultDescription desc,
            EquivToTelescopeResult equivToTelescopeResult
    ) {
        EquivToTelescopeComponent scorerComponent = EquivToTelescopeComponent.create();
        scorerComponent.setComponentName("Sole Candidate Title Matching Container Scorer");
        DefaultScoredCandidates.Builder<Container> equivalents = DefaultScoredCandidates.fromSource(NAME);

        List<Container> potentialExtraCandidates = noContentWithSameTitleAndPublisher(subject);

        if(!potentialExtraCandidates.isEmpty()){
            desc.appendText(
                    "Scored no candidates, as at least one content exists with same title "
                            + "& publisher as the subject (%s)",
                    potentialExtraCandidates.get(0).getCanonicalUri()
            );
            //score all candidates null
            for (Container candidate : candidates) {
                if (candidate.getId() != null) {
                    scorerComponent.addComponentResult(
                            candidate.getId(),
                            String.valueOf(mismatchScore)
                    );
                }
                equivalents.addEquivalent(candidate, mismatchScore);
            }
            equivToTelescopeResult.addScorerResult(scorerComponent);
            return equivalents.build();
        }

        SetMultimap<Publisher, Container> sameTitleCandidates = HashMultimap.create();

        for (Container candidate : candidates) {
            if (titleScorer.calculateScore(subject, candidate).equals(titleScorer.getExactMatchScore())) {
                sameTitleCandidates.put(candidate.getPublisher(), candidate);
            } else {
                addScore(candidate, Score.nullScore(), equivalents, scorerComponent);
            }
        }

        for (Publisher publisher : sameTitleCandidates.keySet()) {
            Set<Container> publisherCandidates = sameTitleCandidates.get(publisher);
            if (publisherCandidates.size() == 1) {
                addScore(publisherCandidates.iterator().next(), exactTitleMatchScore, equivalents, scorerComponent);
            } else {
                for (Container publisherCandidate : publisherCandidates) {
                    addScore(publisherCandidate, mismatchScore, equivalents, scorerComponent);
                }
            }
        }

        equivToTelescopeResult.addScorerResult(scorerComponent);
        return equivalents.build();
    }

    private void addScore(
            Container candidate,
            Score score,
            DefaultScoredCandidates.Builder<Container> equivalents,
            EquivToTelescopeComponent scorerComponent
    ) {
        equivalents.addEquivalent(candidate, score);
        if (candidate.getId() != null) {
            scorerComponent.addComponentResult(
                    candidate.getId(),
                    String.valueOf(score.asDouble())
            );
        }
    }

    private List<Container> noContentWithSameTitleAndPublisher(Container subject) {
        Publisher subjectPublisher = subject.getPublisher();

        Iterable<Container> results = search(subject, subjectPublisher);

        //return actively published results that have the exact same name as the subject (apart from subject itself)
        return stream(results.spliterator(), false)
                .filter(input -> !Objects.equals(subject.getCanonicalUri(), input.getCanonicalUri()))
                .filter(Described::isActivelyPublished)
                .filter(input -> Objects.equals(
                        titleScorer.calculateScore(subject, input),
                        titleScorer.getExactMatchScore()
                ))
                .collect(Collectors.toList());
    }

    //search for top level candidates with exact title match & same publisher as subject
    private Iterable<Container> search(Container subject, Publisher subjectPublisher) {
        SearchQuery.Builder titleQuery =
                SearchQuery.builder(subject.getTitle())
                        .withSelection(new Selection(0, 5))
                        .withTitleWeighting(1.0f)
                        .withPublishers(Collections.singleton(subjectPublisher))
                        .isTopLevelOnly(true);
        Application application = DefaultApplication.createWithReads(Collections.singletonList(subjectPublisher));

        return searchResolver.search(titleQuery.build(), application)
                .stream()
                .filter(Container.class::isInstance)
                .map(Container.class::cast)
                .collect(MoreCollectors.toImmutableList());
    }

    @Override
    public String toString() {
        return "Sole Candidate Title-matching Scorer";
    }

}

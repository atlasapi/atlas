package org.atlasapi.equiv.scorers;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

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

import com.metabroadcast.applications.client.model.internal.Application;
import com.metabroadcast.common.query.Selection;
import com.metabroadcast.common.stream.MoreCollectors;

import com.google.common.base.Functions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

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

    public SoleCandidateTitleMatchContainerScorer(
            Score exactTitleMatchScore,
            Score partialTitleMatchBound,
            SearchResolver searchResolver
    ) {
        this.titleScorer = new ContentTitleScorer<>(
                NAME,
                Functions.<String>identity(),
                exactTitleMatchScore,
                partialTitleMatchBound
        );
        this.searchResolver = searchResolver;
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
        Score equivScore = Score.nullScore();

        List<Container> potentialExtraCandidates = noContentWithSameTitleAndPublisher(subject);

        if(!potentialExtraCandidates.isEmpty()){
            desc.appendText(
                    "Scored no candidates, as at least one content exists with same title "
                            + "& publisher as the subject (%s)",
                    potentialExtraCandidates.get(0).getCanonicalUri()
            );
            //score all candidates null
            for (Container candidate : ImmutableSet.copyOf(candidates)) {
                if (candidate.getId() != null) {
                    scorerComponent.addComponentResult(
                            candidate.getId(),
                            String.valueOf(equivScore)
                    );
                }
                equivalents.addEquivalent(candidate, equivScore);
            }
            equivToTelescopeResult.addScorerResult(scorerComponent);
            return equivalents.build();
        }

        Set<Publisher> scoredPublishers = Sets.newHashSet();

        for (Container candidate : ImmutableSet.copyOf(candidates)) {
            equivScore = Score.nullScore();
            //slight efficiency increase
            if (scoredPublishers.contains(candidate.getPublisher())){
                desc.appendText(
                        "%s not scored, repeat publisher (%s)",
                        candidate.getCanonicalUri(),
                        candidate.getPublisher().title()
                );
            }
            //TODO: score if multiple publishers but IFF 1 of them has exact title match
            else if (isSoleCandidate(candidate, candidates)) {
                equivScore = titleScorer.calculateScore(subject, candidate);
                desc.appendText("%s scored %s on title match", candidate.getCanonicalUri(), equivScore);
                if(equivScore.equals(titleScorer.getExactMatchScore())) {
                    scoredPublishers.add(candidate.getPublisher());
                }
            }
            else {
                //score null the candidates of publishers we know have multiple candidates
                scoredPublishers.add(candidate.getPublisher());
            }
            equivalents.addEquivalent(candidate, equivScore);
            if (candidate.getId() != null) {
                scorerComponent.addComponentResult(
                        candidate.getId(),
                        String.valueOf(equivScore.asDouble())
                );
            }
        }

        equivToTelescopeResult.addScorerResult(scorerComponent);

        return equivalents.build();
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

    //checks if any of the other candidates is from same publisher & has exact same title;
    //if not, then we want to score it (ie. return true)
    private boolean isSoleCandidate(Container candidate, Set<? extends Container> candidates) {
        //it is ok if we only find one match, but bad if we find multiple
        boolean alreadyFoundMatch = false;
        for (Container anotherCandidate : ImmutableSet.copyOf(candidates)) {
            if (anotherCandidate.getPublisher().equals(candidate.getPublisher())
                    && titleScorer.calculateScore(candidate, anotherCandidate)
                    .equals(titleScorer.getExactMatchScore())) {
                if (alreadyFoundMatch) {
                    return false;
                }
                alreadyFoundMatch = true;
            }
        }
        return true;
    }

    @Override
    public String toString() {
        return "Sole Candidate Title-matching Scorer";
    }

}

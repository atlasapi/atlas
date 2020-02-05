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
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Described;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Series;
import org.atlasapi.persistence.content.SearchResolver;
import org.atlasapi.search.model.SearchQuery;

import com.metabroadcast.applications.client.model.internal.Application;
import com.metabroadcast.common.query.Selection;
import com.metabroadcast.common.stream.MoreCollectors;

import com.google.common.base.Functions;
import com.google.common.base.Strings;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.stream.StreamSupport.stream;

/**
 * Score a candidate if exact title match & it is the only candidate from a particular publisher.
 * Uses an inner ContentTitleScorer's logic to calculate if title matches exactly.
 *
 * Searches for exact title + publisher as subject; scores all candidates null if any results found,
 * otherwise scores as above.
 */
public class SoleCandidateTitleMatchingScorer<T extends Content> implements EquivalenceScorer<T>{

    public static final String NAME = "Sole Candidate Title Match";

    private final ContentTitleScorer<T> titleScorer;
    private final SearchResolver searchResolver;
    private final Score exactTitleMatchScore;
    private final Score mismatchScore;
    private final Class<T> clz;

    public SoleCandidateTitleMatchingScorer(
            SearchResolver searchResolver,
            Score exactTitleMatchScore,
            Score mismatchScore,
            Class<T> clz
    ) {
        this.titleScorer = new ContentTitleScorer<>(
                NAME,
                Functions.identity(),
                Score.ONE,
                Score.nullScore()
        );
        this.searchResolver = checkNotNull(searchResolver);
        this.exactTitleMatchScore = checkNotNull(exactTitleMatchScore);
        this.mismatchScore = checkNotNull(mismatchScore);
        this.clz = checkNotNull(clz);
    }

    @Override
    public ScoredCandidates<T> score(
            T subject,
            Set<? extends T> candidates,
            ResultDescription desc,
            EquivToTelescopeResult equivToTelescopeResult
    ) {
        EquivToTelescopeComponent scorerComponent = EquivToTelescopeComponent.create();
        scorerComponent.setComponentName("Sole Candidate Title Matching Container Scorer");
        DefaultScoredCandidates.Builder<T> equivalents = DefaultScoredCandidates.fromSource(NAME);
        if (Strings.isNullOrEmpty(subject.getTitle())) {
            desc.appendText("There was no title, so this Scorer quit.");
            return equivalents.build();
        }

        List<T> potentialExtraCandidates = findContentWithSameTitleAndPublisher(subject);

        if (!potentialExtraCandidates.isEmpty()){
            desc.appendText(
                    "Scored no candidates, as at least one content exists with same title "
                            + "& publisher as the subject (%s)",
                    potentialExtraCandidates.get(0).getCanonicalUri()
            );
            //score all candidates null
            for (T candidate : candidates) {
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

        SetMultimap<Publisher, T> sameTitleCandidates = HashMultimap.create();

        for (T candidate : candidates) {
            if (titleScorer.calculateScore(subject, candidate).equals(titleScorer.getExactMatchScore())) {
                sameTitleCandidates.put(candidate.getPublisher(), candidate);
            } else {
                addScore(candidate, Score.nullScore(), equivalents, scorerComponent);
            }
        }

        for (Publisher publisher : sameTitleCandidates.keySet()) {
            Set<T> publisherCandidates = sameTitleCandidates.get(publisher);
            if (publisherCandidates.size() == 1) {
                //is sole candidate with exact title match from a given publisher
                addScore(publisherCandidates.iterator().next(), exactTitleMatchScore, equivalents, scorerComponent);
            } else {
                for (T publisherCandidate : publisherCandidates) {
                    addScore(publisherCandidate, mismatchScore, equivalents, scorerComponent);
                }
            }
        }

        equivToTelescopeResult.addScorerResult(scorerComponent);
        return equivalents.build();
    }

    private void addScore(
            T candidate,
            Score score,
            DefaultScoredCandidates.Builder<T> equivalents,
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

    private List<T> findContentWithSameTitleAndPublisher(T subject) {
        Publisher subjectPublisher = subject.getPublisher();

        Iterable<T> results = search(subject, subjectPublisher);

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

    //search for candidates with exact title match & same publisher as subject
    private Iterable<T> search(T subject, Publisher subjectPublisher) {
        SearchQuery.Builder titleQuery =
                SearchQuery.builder(subject.getTitle()) //if title is null, this will throw.
                        .withSelection(new Selection(0, 5))
                        .withTitleWeighting(1.0f)
                        .withPublishers(Collections.singleton(subjectPublisher))
                ;
        if(isTopLevelContent(subject)) {
            titleQuery.isTopLevelOnly(true);
        } else {
            titleQuery.withType(subject.getClass().getSimpleName().toLowerCase());
        }
        Application application = DefaultApplication.createWithReads(Collections.singletonList(subjectPublisher));

        return searchResolver.search(titleQuery.build(), application)
                .stream()
                .filter(clz::isInstance)
                .map(clz::cast)
                .collect(MoreCollectors.toImmutableList());
    }

    public static boolean isTopLevelContent(Content content) {
        if(content instanceof Item) {
            if (content instanceof Episode) {
                return false;
            }
            if (((Item) content).getContainer() != null) {
                return false;
            }
        } else if(content instanceof Container) {
            if (content instanceof Series) {
                if (((Series) content).getParent() != null) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public String toString() {
        return "Sole Candidate Title-matching Scorer";
    }

}

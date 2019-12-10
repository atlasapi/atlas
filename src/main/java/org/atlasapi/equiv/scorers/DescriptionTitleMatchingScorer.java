package org.atlasapi.equiv.scorers;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates.Builder;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeComponent;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResult;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Described;
import org.atlasapi.media.entity.Item;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class DescriptionTitleMatchingScorer<T extends Described> implements EquivalenceScorer<T> {

    public static final String NAME = "Description Title Matching Scorer";

    private final Set<String> commonWords = ImmutableSet.of(
            "the", "in", "a", "and", "&", "of", "to", "show", "peppa", "pig"
    );

    private final Score scoreOnMismatch;

    // This can be calibrated to change how often matching occurs
    private final double divisionFactor;

    private DescriptionTitleMatchingScorer(Score scoreOnMismatch, double divisionFactor) {
        this.scoreOnMismatch = scoreOnMismatch;
        this.divisionFactor = divisionFactor;
    }

    public static DescriptionTitleMatchingScorer<Item> createItemScorer() {
        return new DescriptionTitleMatchingScorer<>(Score.nullScore(), 2.0);
    }

    public static DescriptionTitleMatchingScorer<Item> createItemScorer(Score scoreOnMismatch, double divisionFactor) {
        return new DescriptionTitleMatchingScorer<>(scoreOnMismatch, divisionFactor);
    }

    public static DescriptionTitleMatchingScorer<Container> createContainerScorer() {
        return new DescriptionTitleMatchingScorer<>(Score.nullScore(), 2.0);
    }

    public static DescriptionTitleMatchingScorer<Container> createContainerScorer(Score scoreOnMismatch, double divisionFactor) {
        return new DescriptionTitleMatchingScorer<>(scoreOnMismatch, divisionFactor);
    }

    @Override
    public ScoredCandidates<T> score(
            T subject,
            Set<? extends T> suggestions,
            ResultDescription desc,
            EquivToTelescopeResult equivToTelescopeResult
    ) {
        EquivToTelescopeComponent scorerComponent = EquivToTelescopeComponent.create();
        scorerComponent.setComponentName("Description Title Matching Scorer");

        Builder<T> equivalents = DefaultScoredCandidates.fromSource(NAME);

        for (T suggestion : suggestions) {
            Score equivScore = score(subject, suggestion, desc);
            equivalents.addEquivalent(suggestion, equivScore);

            if (suggestion.getId() != null) {
                scorerComponent.addComponentResult(
                        suggestion.getId(),
                        String.valueOf(equivScore)
                );
            }
        }

        equivToTelescopeResult.addScorerResult(scorerComponent);

        return equivalents.build();
    }

    private Score score(T subject, T suggestion, ResultDescription desc) {

        Score score = score(subject, suggestion);
        desc.appendText("%s (%s) scored: %s", suggestion.getTitle(), suggestion.getCanonicalUri(), score);

        return score;
    }

    private Score score(T subject, T suggestion) {
        return descriptionTitleMatch(subject, suggestion)? Score.ONE: Score.nullScore();
    }

    private boolean descriptionTitleMatch(T subject, T candidate) {
        Set<String> candidateList = descriptionToProcessedList(candidate.getDescription());
        Set<String> subjectList = descriptionToProcessedList(subject.getDescription());

        Set<String> candidateTitleList = titleToProcessedList(candidate.getTitle());
        Set<String> subjectTitleList = titleToProcessedList(subject.getTitle());

        subjectList.retainAll(candidateTitleList);
        candidateList.retainAll(subjectTitleList);

        // Calibrate the division factor to alter matching frequency
        return subjectList.size() > (candidateTitleList.size() / divisionFactor) || candidateList.size() > (subjectTitleList.size() / divisionFactor);

    }

    private Set<String> titleToProcessedList(String title) {
        Set<String> titleList = new HashSet<>();
        if (!Strings.isNullOrEmpty(title)) {
            titleList = Arrays.stream(title.replaceAll("[^a-zA-Z0-9 ]", "").toLowerCase().split(" "))
                    .filter( o -> !o.equals(""))
                    .filter(o -> !commonWords.contains(o))
                    .collect(Collectors.toSet());
        }
        return titleList;
    }

    private Set<String> descriptionToProcessedList(String description) {
        Set<String> descriptionList = new HashSet<>();
        if (!Strings.isNullOrEmpty(description)) {
            descriptionList = Arrays.stream(description.replaceAll("[^a-zA-Z0-9 ]", "").split(" "))
                    .filter( o -> !o.equals(""))
                    .filter( o -> Character.isUpperCase(o.charAt(0)))
                    .map(String::toLowerCase)
                    .filter( o -> !commonWords.contains(o))
                    .collect(Collectors.toSet());
        }
        return descriptionList;
    }

    @Override
    public String toString() {
        return "Description-Title-Matching";
    }
}

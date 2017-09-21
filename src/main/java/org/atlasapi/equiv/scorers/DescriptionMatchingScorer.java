package org.atlasapi.equiv.scorers;

import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeComponent;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResults;
import org.atlasapi.media.entity.Equiv;
import org.atlasapi.media.entity.Item;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

public class DescriptionMatchingScorer implements EquivalenceScorer<Item> {

    public static final String NAME = "Description Matching";
    private static final Set<String> commonWords = ImmutableSet.of(
            "the", "in", "a", "and", "&", "of", "to", "show"
    );

    // Proportion threshold of key words that need to match between the two descriptions
    // to conclude there is a match
    public static final double COMMON_WORDS_PROPORTION_THRESHOLD = 0.4;

    // Capitalised word finding regex
    public static final String REGEX = "\\b([A-Z]\\w*)\\b";

    private DescriptionMatchingScorer() {
    }

    public static DescriptionMatchingScorer makeScorer(){
        return new DescriptionMatchingScorer();
    }

    @Override
    public ScoredCandidates<Item> score(
            Item subject,
            Set<? extends Item> candidates,
            ResultDescription desc,
            EquivToTelescopeResults equivToTelescopeResults
    ) {
        EquivToTelescopeComponent scorerComponent = EquivToTelescopeComponent.create();
        scorerComponent.setComponentName("Description Matching Scorer");

        DefaultScoredCandidates.Builder<Item> equivalents =
                DefaultScoredCandidates.fromSource(NAME);

        for (Item candidate : candidates) {
            Score equivScore = score(subject, candidate, desc);
            equivalents.addEquivalent(candidate, equivScore);

            if (candidate.getId() != null) {
                scorerComponent.addComponentResult(
                        candidate.getId(),
                        String.valueOf(equivScore.asDouble())
                );
            }
        }

        equivToTelescopeResults.addScorerResult(scorerComponent);

        return equivalents.build();
    }

    private Score score(Item subject, Item candidate, ResultDescription desc) {
        Score score = score(subject, candidate);
        desc.appendText(
                "%s (%s) scored: %s",
                candidate.getTitle(),
                candidate.getCanonicalUri(),
                score
        );

        return score;
    }

    private Score score(Item subject, Item candidate) {
        return descriptionMatch(subject, candidate) ? Score.ONE : Score.nullScore();
    }

    private boolean descriptionMatch(Item subject, Item candidate) {
        Set<String> candidateList = descriptionToProcessedList(candidate.getDescription());
        Set<String> subjectList = descriptionToProcessedList(subject.getDescription());

        Set<String> allWords = Sets.union(candidateList, subjectList);
        Set<String> commonWords = Sets.intersection(subjectList, candidateList);

        double proportionOfCommonWords = (commonWords.size() + 0.0) / allWords.size();

        return proportionOfCommonWords > COMMON_WORDS_PROPORTION_THRESHOLD;
    }

    private Set<String> descriptionToProcessedList(String description) {
        if (!Strings.isNullOrEmpty(description)) {
            return tokenize(description).stream()
            .filter(s -> !commonWords.contains(s))
            .collect(Collectors.toSet());
        } else {
            return Sets.newHashSet();
        }
    }

    private List<String> tokenize(String target) {
        Pattern pattern = Pattern.compile(REGEX);
        Matcher subjectMatcher = pattern.matcher(target);
        List<String> targetList = new LinkedList<>();
        while (subjectMatcher.find()) {
            targetList.add(subjectMatcher.group(1));
        }
        targetList.replaceAll(string -> string = string.toLowerCase());
        return targetList;
    }

    @Override
    public String toString() {
        return "Description-matching Item Scorer";
    }
}

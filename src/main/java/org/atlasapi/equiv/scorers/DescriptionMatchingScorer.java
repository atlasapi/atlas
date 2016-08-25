package org.atlasapi.equiv.scorers;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.media.entity.Item;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

public class DescriptionMatchingScorer implements EquivalenceScorer<Item> {

    public static final String NAME = "Description Matching";
    private final Set<String> commonWords = ImmutableSet.of(
            "the", "in", "a", "and", "&", "of", "to", "show"
    );

    private DescriptionMatchingScorer() {
    }

    public static DescriptionMatchingScorer makeScorer(){
        return new DescriptionMatchingScorer();
    }

    @Override
    public ScoredCandidates<Item> score(Item subject, Set<? extends Item> candidates,
            ResultDescription desc) {
        DefaultScoredCandidates.Builder<Item> equivalents = DefaultScoredCandidates.fromSource(NAME);

        for (Item candidate : candidates) {
            equivalents.addEquivalent(candidate, score(subject, candidate, desc));
        }

        return equivalents.build();
    }

    private Score score(Item subject, Item candidate, ResultDescription desc) {
        Score score = Score.nullScore();

        score = score(subject, candidate);
        desc.appendText("%s (%s) scored: %s", candidate.getTitle(), candidate.getCanonicalUri(), score);

        return score;
    }

    private Score score(Item subject, Item candidate) {
        return descriptionMatch(subject, candidate)? Score.ONE: Score.nullScore();
    }

    private boolean descriptionMatch(Item subject, Item candidate) {
        List<String> candidateList = descriptionToProcessedList(candidate.getDescription());
        List<String> subjectList = descriptionToProcessedList(subject.getDescription());

        candidateList.retainAll(subjectList);

        //calibrate here
        return new Double(candidateList.size())/new Double(subjectList.size()) > 0.2;
    }

    private List<String> descriptionToProcessedList(String description) {
        List<String> descriptionList = new LinkedList<>();
        if (!Strings.isNullOrEmpty(description)) {
            descriptionList = filterCommon(new HashSet<>(tokenize(description)));
        }
        return descriptionList;
    }

    private List<String> filterCommon(Set<String> words) {
        return new ArrayList(Sets.difference(words, commonWords));
    }

    private List<String> tokenize(String target) {
        Pattern pattern = Pattern.compile("\\b([A-Z]\\w*)\\b");
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

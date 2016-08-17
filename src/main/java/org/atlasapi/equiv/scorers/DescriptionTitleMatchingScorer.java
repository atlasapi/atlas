package org.atlasapi.equiv.scorers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;

import org.atlasapi.equiv.generators.ExpandingTitleTransformer;
import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates.Builder;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.media.entity.Item;

import com.google.common.base.Strings;
import com.google.common.collect.Sets;

public class DescriptionTitleMatchingScorer implements EquivalenceScorer<Item> {

    public static final String NAME = "Description title matching";
    private final ExpandingTitleTransformer titleExpander = new ExpandingTitleTransformer();

    private final Set<String> commonWords = ImmutableSet.of(
            "the", "in", "a", "and", "&", "of", "to", "show"
    );


    private final Score scoreOnMismatch;

    public DescriptionTitleMatchingScorer() {
        this(Score.NULL_SCORE);
    }

    public DescriptionTitleMatchingScorer(Score scoreOnMismatch) {
        this.scoreOnMismatch = scoreOnMismatch;
    }

    @Override
    public ScoredCandidates<Item> score(Item subject, Set<? extends Item> suggestions, ResultDescription desc) {
        Builder<Item> equivalents = DefaultScoredCandidates.fromSource(NAME);

        for (Item suggestion : suggestions) {
            equivalents.addEquivalent(suggestion, score(subject, suggestion, desc));
        }

        return equivalents.build();
    }

    private Score score(Item subject, Item suggestion, ResultDescription desc) {
        Score score = Score.NULL_SCORE;

        score = score(subject, suggestion);
        desc.appendText("%s (%s) scored: %s", suggestion.getTitle(), suggestion.getCanonicalUri(), score);

        return score;
    }


    private Score score(Item subject, Item suggestion) {

        return descriptionTitleMatch(subject, suggestion)? Score.ONE: Score.nullScore();
    }

    protected boolean descriptionTitleMatch(Item subject, Item candidate) {


        List<String> candidateList = descriptionToProcessedList(candidate.getDescription());
        List<String> subjectList = descriptionToProcessedList(subject.getDescription());

        List candidateTitleList = titleToProcessedList(candidate.getTitle());
        List subjectTitleList = titleToProcessedList(subject.getTitle());

        subjectList.retainAll(candidateTitleList);
        candidateList.retainAll(subjectTitleList);

        // calibrate the division factor
        return subjectList.size() > (candidateTitleList.size()/2) || candidateList.size() > (subjectTitleList.size()/2);

    }

    private List<String> titleToProcessedList(String title) {
        List<String> titleList = new LinkedList<>();
        if (!Strings.isNullOrEmpty(title)) {
            titleList = filterCommon(new HashSet<>(Arrays.asList(title.toLowerCase().split(" "))));
        }
        return titleList;
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
        return "Description-title-matching Item Scorer";
    }
}

package org.atlasapi.equiv.scorers.proposedtitlematching;

import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.ImmutableSet;

import org.atlasapi.equiv.generators.ExpandingTitleTransformer;
import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates.Builder;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.equiv.scorers.EquivalenceScorer;
import org.atlasapi.media.entity.Item;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;

public class DescriptionMatching implements EquivalenceScorer<Item> {

    public static final String NAME = "Description";
    private final ExpandingTitleTransformer titleExpander = new ExpandingTitleTransformer();


    private final Score scoreOnMismatch;

    public DescriptionMatching() {
        this(Score.NULL_SCORE);
    }

    public DescriptionMatching(Score scoreOnMismatch) {
        this.scoreOnMismatch = scoreOnMismatch;
    }

    @Override
    public ScoredCandidates<Item> score(Item subject, Set<? extends Item> suggestions, ResultDescription desc) {
        Builder<Item> equivalents = DefaultScoredCandidates.fromSource(NAME);

        if(Strings.isNullOrEmpty(subject.getDescription())) {
            desc.appendText("No Description on subject, all score null");
        }

        for (Item suggestion : suggestions) {
            equivalents.addEquivalent(suggestion, score(subject, suggestion, desc));
        }

        return equivalents.build();
    }

    private Score score(Item subject, Item suggestion, ResultDescription desc) {
        Score score = Score.NULL_SCORE;
        if(Strings.isNullOrEmpty(suggestion.getTitle())) {
            desc.appendText("No Title (%s) scored: %s", suggestion.getCanonicalUri(), score);
        } else {
            score = score(subject, suggestion);
            desc.appendText("%s (%s) scored: %s", suggestion.getTitle(), suggestion.getCanonicalUri(), score);
        }
        return score;
    }


    private Score score(Item subject, Item suggestion) {

        return descriptionMatch(subject, suggestion)? Score.valueOf(2.0): Score.nullScore();
    }

    protected boolean descriptionMatch(Item subject, Item candidate) {
        if (subject.getDescription() == null || candidate.getDescription() == null || subject.getDescription().isEmpty() || candidate.getDescription().isEmpty()) {
            return false;
        }

        Pattern pattern = Pattern.compile("\\b([A-Z]\\w*)\\b");
        Matcher subjectMatcher = pattern.matcher(subject.getDescription());
        Matcher candidateMatcher = pattern.matcher(candidate.getDescription());
        List<String> subjectList = new LinkedList<>();
        List<String> candidateList = new LinkedList<>();
        while (subjectMatcher.find()) {
            subjectList.add(subjectMatcher.group(1));
        }
        while (candidateMatcher.find()) {
            candidateList.add(candidateMatcher.group(1));
        }
        // check if the average size of capitalised words is less than
        // the words found in both descriptions
        double averageSize = (subjectList.size() + candidateList.size()) / 2;
        subjectList.retainAll(candidateList);
        // calibrate the multiplication factor
        return (subjectList.size() * 3) > averageSize;

    }

    @Override
    public String toString() {
        return "Description-matching Item Scorer";
    }
}

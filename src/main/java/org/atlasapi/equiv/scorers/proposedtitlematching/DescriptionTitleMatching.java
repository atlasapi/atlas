package org.atlasapi.equiv.scorers.proposedtitlematching;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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

public class DescriptionTitleMatching implements EquivalenceScorer<Item> {

    public static final String NAME = "Description title matching";
    private final ExpandingTitleTransformer titleExpander = new ExpandingTitleTransformer();


    private final Score scoreOnMismatch;

    public DescriptionTitleMatching() {
        this(Score.NULL_SCORE);
    }

    public DescriptionTitleMatching(Score scoreOnMismatch) {
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

        return descriptionTitleMatch(subject, suggestion)? Score.valueOf(2.0): Score.nullScore();
    }

    protected boolean descriptionTitleMatch(Item subject, Item candidate) {


        List<String> candidateList = new LinkedList<>();
        List<String> subjectList = new LinkedList<>();
        if (!Strings.isNullOrEmpty(subject.getDescription())) {
            subjectList = tokenize(subject.getDescription());
        }
        if (!Strings.isNullOrEmpty(candidate.getDescription())) {
            candidateList = tokenize(candidate.getDescription());
        }

        List candidateTitleList = new ArrayList<>();
        List subjectTitleList = new ArrayList<>();
        if (!Strings.isNullOrEmpty(candidate.getTitle())){
            candidateTitleList = Arrays.asList(candidate.getTitle().split(" "));
        }
        if (!Strings.isNullOrEmpty(subject.getTitle())) {
            subjectTitleList = Arrays.asList(subject.getTitle().split(" "));
        }

        subjectList.retainAll(candidateTitleList);
        candidateList.retainAll(subjectTitleList);

        // calibrate the multiplication factor
        return subjectList.size() > (candidateTitleList.size()/2) || candidateList.size() > (subjectTitleList.size()/2);

    }

    public List<String> tokenize(String target) {
        Pattern pattern = Pattern.compile("\\b([A-Z]\\w*)\\b");
        Matcher subjectMatcher = pattern.matcher(target);
        List<String> targetList = new LinkedList<>();
        while (subjectMatcher.find()) {
            targetList.add(subjectMatcher.group(1));
        }
        return targetList;
    }

    @Override
    public String toString() {
        return "Description-title-matching Item Scorer";
    }
}

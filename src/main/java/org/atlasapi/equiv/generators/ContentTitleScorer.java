package org.atlasapi.equiv.generators;

import javax.annotation.Nonnull;

import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates.Builder;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeComponent;
import org.atlasapi.media.entity.Content;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

public final class ContentTitleScorer<T extends Content> {
    
    private final Function<String, String> titleTransform;
    private final String name;
    private final Score exactMatchScore;
    private final Score partialMatchUpperBound;
    private final boolean scaleOnPartialMatch; //if true, then add harsh penalty for partial match

    public ContentTitleScorer(
            String name,
            Function<String, String> titleTransform,
            Score exactMatchScore,
            Score partialMatchUpperBound
    ) {
        this(name, titleTransform, exactMatchScore, partialMatchUpperBound, true);
    }

    public ContentTitleScorer(
            String name,
            Function<String, String> titleTransform,
            Score exactMatchScore,
            Score partialMatchUpperBound,
            boolean scaleOnPartialMatch
    ) {
        this.name = name;
        this.titleTransform = titleTransform;
        this.exactMatchScore = exactMatchScore;
        this.partialMatchUpperBound = partialMatchUpperBound;
        this.scaleOnPartialMatch = scaleOnPartialMatch;
    }

    public ScoredCandidates<T> scoreCandidates(
            T content, Iterable<? extends T> candidates,
            ResultDescription desc,
            EquivToTelescopeComponent generatorComponent
    ) {
        Builder<T> equivalents = DefaultScoredCandidates.fromSource(name);
        desc.appendText("Scoring %s candidates", Iterables.size(candidates));
        
        for (T found : ImmutableSet.copyOf(candidates)) {
            Score equivScore = score(content, found, desc);
            equivalents.addEquivalent(found, equivScore);

            if (found.getId() != null) {
                generatorComponent.addComponentResult(
                        found.getId(),
                        String.valueOf(equivScore.asDouble())
                );
            }
        }

        return equivalents.build();
    }
    
    /**
     * Calculates a score representing the similarity of the candidate's title compared to the subject's title.
     * @param subject - subject content
     * @param candidate - candidate content
     * @return score representing how closely candidate's title matches subject's title.
     */
    public Score score(Content subject, Content candidate, ResultDescription desc) {
        if (subject.getTitle() == null || candidate.getTitle() == null) {
            return Score.nullScore();
        }
        String subjectTitle = sanitize(subject.getTitle());
        String contentTitle = sanitize(candidate.getTitle());
        Score score = score(subjectTitle, contentTitle);
        desc.appendText("%s vs. %s (%s): %s", subjectTitle, contentTitle, candidate.getCanonicalUri(), score);
        return score;
    }

    public Score calculateScore(Content subject, Content candidate) {
        if (subject.getTitle() == null || candidate.getTitle() == null) {
            return Score.nullScore();
        }
        String subjectTitle = sanitize(subject.getTitle());
        String contentTitle = sanitize(candidate.getTitle());
        return score(subjectTitle, contentTitle);
    }

    @Nonnull
    private String sanitize(@Nonnull String title) {
        return removeCommonPrefixes(titleTransform.apply(title)
            .replaceAll(" & ", " and ")
            .replaceAll("[^\\d\\w\\s]", "").toLowerCase());
    }
    
    private String removeCommonPrefixes(String title) {
        return (title.startsWith("the ") ? title.substring(4) : title).replace(" ", "");
    }
    
    private Score score(String subjectTitle, String candidateTitle) {
        if (subjectTitle.length() < candidateTitle.length()) {
            return scoreTitles(subjectTitle, candidateTitle);
        } else {
            return scoreTitles(candidateTitle, subjectTitle);
        }
    }

    private Score scoreTitles(String shorter, String longer) {
        int commonPrefix = commonPrefixLength(shorter, longer);
        int difference = longer.length() - commonPrefix;
        if (difference == 0) {
            return exactMatchScore;
        }
        if (partialMatchUpperBound.isRealScore() && scaleOnPartialMatch) {
            return Score.valueOf(
                    partialMatchUpperBound.asDouble()
                            / (Math.exp(Math.pow(difference, 2)) + 8 * difference)
            );
        } else {
            return partialMatchUpperBound;
        }
    }

    private int commonPrefixLength(String t1, String t2) {
        int i = 0;
        for (; i < Math.min(t1.length(), t2.length()) && t1.charAt(i) == t2.charAt(i); i++) {
        }
        return i;
    }

    public Score getExactMatchScore(){
        return exactMatchScore;
    }

}

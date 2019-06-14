package org.atlasapi.equiv.generators;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates;
import org.atlasapi.equiv.results.scores.DefaultScoredCandidates.Builder;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.results.scores.ScoredCandidates;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeComponent;
import org.atlasapi.media.entity.Content;

public final class ContentTitleScorer<T extends Content> {
    
    private final Function<String, String> titleTransform;
    private final String name;
    private final double exactMatchScore;

    public ContentTitleScorer(String name, Function<String, String> titleTransform, double exactMatchScore) {
        this.name = name;
        this.titleTransform = titleTransform;
        this.exactMatchScore = exactMatchScore;
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
            return Score.NULL_SCORE;
        }
        String subjectTitle = sanitize(subject.getTitle());
        String contentTitle = sanitize(candidate.getTitle());
        double score = score(subjectTitle, contentTitle);
        desc.appendText("%s vs. %s (%s): %s", subjectTitle, contentTitle, candidate.getCanonicalUri(), score);
        return Score.valueOf(score);
    }
    
    private String sanitize(String title) {
        return removeCommonPrefixes(titleTransform.apply(title)
            .replaceAll(" & ", " and ")
            .replaceAll("[^\\d\\w\\s]", "").toLowerCase());
    }
    
    private String removeCommonPrefixes(String title) {
        return (title.startsWith("the ") ? title.substring(4) : title).replace(" ", "");
    }
    
    private double score(String subjectTitle, String candidateTitle) {
        if (subjectTitle.length() < candidateTitle.length()) {
            return scoreTitles(subjectTitle, candidateTitle);
        } else {
            return scoreTitles(candidateTitle, subjectTitle);
        }
    }

    private double scoreTitles(String shorter, String longer) {
        int commonPrefix = commonPrefixLength(shorter, longer);
        int difference = longer.length() - commonPrefix;
        if (difference == 0) {
            return exactMatchScore;
        }
        return 1.0 / (Math.exp(Math.pow(difference, 2)) + 8*difference);
    }

    private int commonPrefixLength(String t1, String t2) {
        int i = 0;
        for (; i < Math.min(t1.length(), t2.length()) && t1.charAt(i) == t2.charAt(i); i++) {
        }
        return i;
    }
}

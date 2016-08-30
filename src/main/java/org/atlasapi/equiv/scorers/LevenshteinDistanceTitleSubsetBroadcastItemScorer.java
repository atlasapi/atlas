package org.atlasapi.equiv.scorers;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Collection;
import java.util.Set;

import org.atlasapi.equiv.generators.ExpandingTitleTransformer;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.equiv.scorers.BaseBroadcastItemScorer;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Item;
import org.atlasapi.persistence.content.ContentResolver;

import com.google.common.base.CharMatcher;
import com.google.common.base.Function;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Ordering;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import org.apache.commons.lang.StringUtils;

/**
 * <p>
 * A {@link BaseBroadcastItemScorer} which matches if a percentage of the words
 * in the title with fewer words are contained in the other title. If the titles
 * have the same word count then the title from the subject content is used.
 * </p>
 * <p>
 * Common words are ignored. Words are trimmed of non-
 * {@link CharMatcher#JAVA_LETTER} characters.
 * </p>
 */
public final class LevenshteinDistanceTitleSubsetBroadcastItemScorer extends BaseBroadcastItemScorer {

    public static final String NAME = "LD-Broadcast-Title-Subset";

    private final ExpandingTitleTransformer titleTransformer = new ExpandingTitleTransformer();

    private final CharMatcher punctuation = CharMatcher.JAVA_LETTER.negate();
    private final Splitter splitter = Splitter.on(' ')
            .trimResults(punctuation)
            .omitEmptyStrings();

    private final Ordering<Collection<?>> collectionSize = Ordering.natural()
            .onResultOf(new Function<Collection<?>, Integer>() {
                @Override
                public Integer apply(Collection<?> input) {
                    return input.size();
                }
            });
    private final Set<String> commonWords = ImmutableSet.of(
            "the", "in", "a", "and", "&", "of", "to", "show"
    );

    private final double threshold;

    /**
     * <p>Creates a new TitleSubsetBroadcastItemScorer which scores based on the
     * number of words in one title occurring in the other.</p>
     *
     * @param resolver
     *            - used to find containers of the subject and candidates.
     * @param misMatchScore
     *            - the score to use in case all matches fail.
     * @param percentThreshold
     *            - the percent of words in the shorter title required to be in
     *            the longer title for a match to succeed.
     */
    public LevenshteinDistanceTitleSubsetBroadcastItemScorer(ContentResolver resolver, Score misMatchScore, int percentThreshold) {
        super(resolver, misMatchScore);
        Range<Integer> percentRange = Range.closed(0, 100);
        checkArgument(percentRange.contains(percentThreshold),
                "%s must be in %s", percentThreshold, percentRange);
        this.threshold = percentThreshold/100.0;
    }

    @Override
    protected String getName() {
        return NAME;
    }

    @Override
    protected boolean subjectAndCandidateMatch(Item subject, Item candidate) {
        return subsetOfShorterTitleContainedInLonger(subject, candidate);
    }

    @Override
    protected boolean subjectContainerAndCandidateMatch(Container subjectContainer, Item candidate) {
        return subsetOfShorterTitleContainedInLonger(subjectContainer, candidate);
    }

    @Override
    protected boolean subjectAndCandidateContainerMatch(Item subject, Container candidateContainer) {
        return subsetOfShorterTitleContainedInLonger(subject, candidateContainer);
    }

    private boolean subsetOfShorterTitleContainedInLonger(Content subject, Content candidate) {
        if (titleMissing(subject) || titleMissing(candidate)) {
            return false;
        }
        String sanitizedSubjectTitle = sanitize(subject.getTitle());
        String sanitizedCandidateTitle = sanitize(candidate.getTitle());
        Set<String> subjectWords = filterCommon(titleWords(sanitizedSubjectTitle));
        Set<String> candidateWords = filterCommon(titleWords(sanitizedCandidateTitle));
        double lDistance = (double) StringUtils.getLevenshteinDistance(
                subjectWords.toString(),
                candidateWords.toString());
        double stringsTotalLength = (double) (
                subjectWords.toString().length() + candidateWords.toString().length()
        );
        return 1 - (lDistance / stringsTotalLength) >= threshold;
    }

    private String sanitize(String title) {
        return titleTransformer.expand(title)
                //strip punctuation
                .replaceAll("[^\\d\\w\\s]", "").toLowerCase();
    }

    private Set<String> filterCommon(Set<String> words) {
        return Sets.difference(words, commonWords);
    }

    private ImmutableSet<String> titleWords(String title) {
        return ImmutableSet.copyOf(splitter.split(title));
    }

    protected boolean descriptionMatch(Item subject, Item candidate){ return false; }

    private boolean titleMissing(Content subject) {
        return Strings.isNullOrEmpty(subject.getTitle());
    }
}

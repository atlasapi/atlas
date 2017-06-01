package org.atlasapi.equiv.scorers;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Collection;
import java.util.Set;

import org.atlasapi.equiv.generators.ExpandingTitleTransformer;
import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Item;
import org.atlasapi.persistence.content.ContentResolver;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Ordering;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;

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
 * <p>
 * Before performing the sub-set match, an exact (case-insensitive) comparison
 * will be performed, to catch simple cases where titles are made up purely of
 * common words and/or non-letter characters (e.g. "The 100").
 * </p>
 */
public final class TitleSubsetBroadcastItemScorer extends BaseBroadcastItemScorer {

    public static final String NAME = "Broadcast-Title-Subset";

    private final ExpandingTitleTransformer titleTransformer = new ExpandingTitleTransformer();

    private final CharMatcher punctuation = CharMatcher.JAVA_LETTER.negate();
    private final Splitter splitter = Splitter.on(' ')
            .trimResults(punctuation)
            .omitEmptyStrings();

    private final Ordering<Collection<?>> collectionSize = Ordering.natural()
            .onResultOf(Collection::size);
    private final Set<String> commonWords = ImmutableSet.of(
        "the", "in", "a", "an","and", "of", "to", "show"
    );  // don't bother with things like "&" as we only care about letter characters

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
    public TitleSubsetBroadcastItemScorer(ContentResolver resolver, Score misMatchScore, int percentThreshold) {
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
        return titlesMatch(subject, candidate);
    }

    @Override
    protected boolean subjectContainerAndCandidateMatch(Container subjectContainer, Item candidate) {
        return titlesMatch(subjectContainer, candidate);
    }

    @Override
    protected boolean subjectAndCandidateContainerMatch(Item subject, Container candidateContainer) {
        return titlesMatch(subject, candidateContainer);
    }

    private boolean titlesMatch(Content subject, Content candidate) {
        if (titleMissing(subject) || titleMissing(candidate)) {
            return false;
        }
        String subjectTitle = sanitize(subject.getTitle());
        String candidateTitle = sanitize(candidate.getTitle());
        return titlesEqual(subjectTitle, candidateTitle)
                || subsetOfShorterInLonger(subjectTitle, candidateTitle);
    }

    private boolean titleMissing(Content subject) {
        return Strings.isNullOrEmpty(subject.getTitle());
    }

    private String sanitize(String title) {
        return titleTransformer.expand(title).toLowerCase();
    }

    private boolean titlesEqual(String subjectTitle, String candidateTitle) {
        return subjectTitle.equals(candidateTitle);
    }

    private boolean subsetOfShorterInLonger(String subjectTitle, String candidateTitle) {
        Set<String> subjectWords = filterCommon(titleWords(subjectTitle));
        Set<String> candidateWords = filterCommon(titleWords(candidateTitle));
        Set<String> shorter = collectionSize.min(subjectWords, candidateWords);
        Set<String> longer = collectionSize.max(candidateWords, subjectWords);
        return percentOfShorterInLonger(shorter, longer) >= threshold;
    }

    private ImmutableSet<String> titleWords(String title) {
        return ImmutableSet.copyOf(splitter.split(
                title.replaceAll("[^\\d\\w\\s]", "")
        ));
    }

    private Set<String> filterCommon(Set<String> words) {
        return Sets.difference(words, commonWords);
    }

    private double percentOfShorterInLonger(Set<String> shorter, Set<String> longer) {
        int contained = Sets.intersection(shorter, longer).size();
        return (contained * 1.0) / shorter.size();
    }

}

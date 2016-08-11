package org.atlasapi.equiv.scorers;

import org.atlasapi.equiv.results.scores.Score;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Item;
import org.atlasapi.persistence.content.ContentResolver;

import com.google.common.base.Strings;

/**
 * <p>
 * A {@link BaseBroadcastItemScorer} which checks for exact title matches in all
 * checks. Missing/empty titles will mis-match.
 * </p>
 */
public class BroadcastItemTitleScorer extends BaseBroadcastItemScorer {

    public static final String NAME = "Broadcast-Title";

    public BroadcastItemTitleScorer(ContentResolver resolver) {
        this(resolver, Score.nullScore());
    }

    public BroadcastItemTitleScorer(ContentResolver resolver, Score misMatchScore) {
        super(resolver, misMatchScore);
    }

    @Override
    protected String getName() {
        return NAME;
    }

    @Override
    protected boolean subjectAndCandidateContainerMatch(Item subject, Container candidateContainer) {
        return equalTitles(subject, candidateContainer);
    }

    @Override
    protected boolean subjectContainerAndCandidateMatch(Container subjectContainer, Item candidate) {
        return equalTitles(candidate, subjectContainer);
    }

    @Override
    protected boolean subjectAndCandidateMatch(Item subject, Item candidate) {
        return equalTitles(subject, candidate);
    }

    private boolean equalTitles(Content c1, Content c2) {
        return hasTitle(c1) && c1.getTitle().equals(c2.getTitle());
    }

    private boolean hasTitle(Content c) {
        return !Strings.isNullOrEmpty(c.getTitle());
    }
    protected boolean descriptionMatch(Item subject, Item candidate){ return false; }

}

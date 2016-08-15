package org.atlasapi.equiv.results.filters;

import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.ScoredCandidate;
import org.atlasapi.media.entity.Content;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UnpublishedContentFilter<T extends Content> extends AbstractEquivalenceFilter<T> {

    private final static Logger log = LoggerFactory.getLogger(UnpublishedContentFilter.class);

    @Override
    public boolean doFilter(ScoredCandidate<T> candidate, T subject, ResultDescription desc) {
        T equivalent = candidate.candidate();

        boolean isActivelyPublished = equivalent.isActivelyPublished();

        if (!isActivelyPublished) {
            desc.appendText("%s removed. Is not actively published",
                    equivalent);
            log.info("Candidate {} was filtered out because it was not actively published",
                    equivalent.getCanonicalUri());
        }
        return isActivelyPublished;
    }

    @Override
    public String toString() {
        return "Specialization matching filter";
    }

}

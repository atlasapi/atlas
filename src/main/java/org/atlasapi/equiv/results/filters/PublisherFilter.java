package org.atlasapi.equiv.results.filters;

import java.util.Map;
import java.util.Set;

import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.ScoredCandidate;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Publisher;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public class PublisherFilter<T extends Content> extends AbstractEquivalenceFilter<T> {

    Map<Publisher, Set<Publisher>> unacceptablePublishers = ImmutableMap.<Publisher, Set<Publisher>>of(
        Publisher.BBC,  ImmutableSet.of(Publisher.C4_PMLSD, Publisher.ITV, Publisher.FIVE),
        Publisher.C4_PMLSD,   ImmutableSet.of(Publisher.BBC, Publisher.ITV, Publisher.FIVE),
        Publisher.ITV,  ImmutableSet.of(Publisher.BBC, Publisher.C4_PMLSD, Publisher.FIVE),
        Publisher.FIVE, ImmutableSet.of(Publisher.BBC, Publisher.C4_PMLSD, Publisher.ITV));
    
    protected boolean doFilter(ScoredCandidate<T> candidate, T subject, ResultDescription desc) {
        if (candidate.candidate().getPublisher() == subject.getPublisher()) {
            return false;
        }
        Set<Publisher> unacceptable = unacceptablePublishers.get(subject.getPublisher());
        if (unacceptable == null) {
            return true;
        }

        boolean passes = !unacceptable.contains(candidate.candidate().getPublisher());
        if (!passes) {
            desc.appendText("%s removed. %s ∈ %s", candidate, candidate.candidate().getPublisher(), unacceptable);
        }
        return passes;
    }

    @Override
    public String toString() {
        return "Publisher filter";
    }
}

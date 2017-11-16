package org.atlasapi.equiv.results.filters;

import java.util.Map;
import java.util.Set;

import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.ScoredCandidate;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeComponent;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResults;
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
    
    protected boolean doFilter(
            ScoredCandidate<T> candidate,
            T subject,
            ResultDescription desc,
            EquivToTelescopeResults equivToTelescopeResults
    ) {
        EquivToTelescopeComponent filterComponent = EquivToTelescopeComponent.create();
        filterComponent.setComponentName("Publisher Filter");

        if (candidate.candidate().getPublisher() == subject.getPublisher()) {
            filterComponent.addComponentResult(
                    candidate.candidate().getId(),
                    "Removing because target and source have the same publisher."
            );
            equivToTelescopeResults.addFilterResult(filterComponent);
            return false;
        }
        Set<Publisher> unacceptable = unacceptablePublishers.get(subject.getPublisher());
        if (unacceptable == null) {
            filterComponent.addComponentResult(
                    candidate.candidate().getId(),
                    "Went through."
            );
            equivToTelescopeResults.addFilterResult(filterComponent);
            return true;
        }

        boolean passes = !unacceptable.contains(candidate.candidate().getPublisher());
        if (!passes) {
            desc.appendText(
                    "%s removed. %s ∈ %s",
                    candidate,
                    candidate.candidate().getPublisher(),
                    unacceptable
            );
            if (candidate.candidate().getId() != null) {
                filterComponent.addComponentResult(
                        candidate.candidate().getId(),
                        "Removing for containing unacceptable publisher"
                                + candidate.candidate().getPublisher().toString()
                );
            }
        } else {
            filterComponent.addComponentResult(
                    candidate.candidate().getId(),
                    "Went through."
            );
        }

        equivToTelescopeResults.addFilterResult(filterComponent);

        return passes;
    }
}

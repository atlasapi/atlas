package org.atlasapi.equiv.results.filters;

import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.ScoredCandidate;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeComponent;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResults;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Equiv;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UnpublishedContentFilter<T extends Content> extends AbstractEquivalenceFilter<T> {

    private final static Logger log = LoggerFactory.getLogger(UnpublishedContentFilter.class);

    @Override
    public boolean doFilter(
            ScoredCandidate<T> candidate,
            T subject,
            ResultDescription desc,
            EquivToTelescopeResults equivToTelescopeResults
    ) {
        EquivToTelescopeComponent filterCompoenent = EquivToTelescopeComponent.create();
        filterCompoenent.setComponentName("Unpublished Content Filter");

        T equivalent = candidate.candidate();

        boolean isActivelyPublished = equivalent.isActivelyPublished();

        if (!isActivelyPublished) {
            desc.appendText("%s removed. Is not actively published",
                    equivalent);
            log.info("Candidate {} was filtered out because it was not actively published",
                    equivalent.getCanonicalUri());
            if (equivalent.getId() != null) {
                filterCompoenent.addComponentResult(
                        equivalent.getId(),
                        "Removed for not being actively published"
                );
            }
        } else {
            filterCompoenent.addComponentResult(
                    equivalent.getId(),
                    "Went through."
            );
        }

        equivToTelescopeResults.addFilterResult(filterCompoenent);

        return isActivelyPublished;
    }
}

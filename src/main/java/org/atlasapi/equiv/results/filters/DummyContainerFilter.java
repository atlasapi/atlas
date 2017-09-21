package org.atlasapi.equiv.results.filters;

import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.ScoredCandidate;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeComponent;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResults;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Content;

public class DummyContainerFilter<T extends Content> extends AbstractEquivalenceFilter<T> {

    @Override
    protected boolean doFilter(
            ScoredCandidate<T> input,
            T subject,
            ResultDescription desc,
            EquivToTelescopeResults equivToTelescopeResults
    ) {
        EquivToTelescopeComponent filterComponent = EquivToTelescopeComponent.create();
        filterComponent.setComponentName("Dummy Container Filter");

        if (!(input.candidate() instanceof Container && subject instanceof Container)) {
            return true;
        }

        Container subjectContainer = (Container) subject;
        Container candidateContainer = (Container) input.candidate();

        boolean retain = haveValuableInformation(subjectContainer, candidateContainer);

        if (!retain) {
            desc.appendText(
                    "%s and %s removed. One of the containers is dummy and shouldn't "
                            + "be equivalated.",
                    subjectContainer,
                    candidateContainer
            );
            if (candidateContainer.getId() != null) {
                filterComponent.addComponentResult(
                        candidateContainer.getId(),
                        "Removed as dummy (empty) container"
                );
            }
        }

        equivToTelescopeResults.addFilterResult(filterComponent);

        return retain;
    }

    private boolean haveValuableInformation(Container subjectContainer,
            Container candidateContainer) {
        return (!subjectContainer.getChildRefs().isEmpty() || !subjectContainer.people()
                .isEmpty()) && (!candidateContainer.getChildRefs().isEmpty()
                || !candidateContainer.people().isEmpty());
    }
}

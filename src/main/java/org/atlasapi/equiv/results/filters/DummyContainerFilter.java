package org.atlasapi.equiv.results.filters;

import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.ScoredCandidate;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Content;

public class DummyContainerFilter<T extends Content> extends AbstractEquivalenceFilter<T> {

    @Override
    protected boolean doFilter(ScoredCandidate<T> input, T subject, ResultDescription desc) {

        if (!(input.candidate() instanceof Container && subject instanceof Container)) {
            return true;
        }

        Container subjectContainer = (Container) subject;
        Container candidateContainer = (Container) input.candidate();

        boolean retain = haveValuableInformation(subjectContainer, candidateContainer);

        if (!retain) {
            desc.appendText("%s and %s removed. One of the containers is dummy and shouldn't be equivalated.",
                    subjectContainer, candidateContainer);
        }

        return retain;
    }

    private boolean haveValuableInformation(Container subjectContainer,
            Container candidateContainer) {
        return (!subjectContainer.getChildRefs().isEmpty() || !subjectContainer.people()
                .isEmpty()) && (!candidateContainer.getChildRefs().isEmpty()
                || !candidateContainer.people().isEmpty());
    }

    @Override
    public String toString() {
        return "Dummy container filter";
    }

}

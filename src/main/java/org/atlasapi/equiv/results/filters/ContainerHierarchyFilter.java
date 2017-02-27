package org.atlasapi.equiv.results.filters;

import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.ScoredCandidate;
import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Series;

/**
 * Filter which ensures only top-level containers can match top-level containers
 * and non-top-level only match non-top-level.
 */
public class ContainerHierarchyFilter extends AbstractEquivalenceFilter<Container> {

    @Override
    protected boolean doFilter(ScoredCandidate<Container> input, Container subject,
            ResultDescription desc) {
        boolean retain = isTopLevel(subject) == isTopLevel(input.candidate());
        if (!retain) {
            desc.appendText("remove %s", input.candidate());
        }
        return retain;
    }
    
    private boolean isTopLevel(Container container) {
        return container instanceof Brand || topLevelSeries(container);
    }

    private boolean topLevelSeries(Container container) {
        return container instanceof Series 
            && ((Series) container).getParent() == null;
    }
}

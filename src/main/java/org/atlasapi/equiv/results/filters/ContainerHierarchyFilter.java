package org.atlasapi.equiv.results.filters;

import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.ScoredCandidate;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeComponent;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResults;
import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Series;

/**
 * Filter which ensures only top-level containers can match top-level containers
 * and non-top-level only match non-top-level.
 */
public class ContainerHierarchyFilter extends AbstractEquivalenceFilter<Container> {

    @Override
    protected boolean doFilter(
            ScoredCandidate<Container> input,
            Container subject,
            ResultDescription desc,
            EquivToTelescopeResults equivToTelescopeResults
    ) {
        EquivToTelescopeComponent filterComponent = EquivToTelescopeComponent.create();
        filterComponent.setComponentName("Container Hierarchy Filter");

        boolean retain = isTopLevel(subject) == isTopLevel(input.candidate());
        if (!retain) {
            desc.appendText("remove %s", input.candidate());
            filterComponent.addComponentResult(
                    input.candidate().getId(),
                    "Removed for not being top level"
            );
        }
        equivToTelescopeResults.addFilterResult(filterComponent);

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

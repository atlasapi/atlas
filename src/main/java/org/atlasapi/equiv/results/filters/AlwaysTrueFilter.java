package org.atlasapi.equiv.results.filters;

import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.ScoredCandidate;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeComponent;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResults;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Identified;

public class AlwaysTrueFilter<T> extends AbstractEquivalenceFilter<T> {

    private static final EquivalenceFilter<Object> INSTANCE 
                                        = new AlwaysTrueFilter<Object>();

    @SuppressWarnings("unchecked")
    public static final <T> EquivalenceFilter<T> get() {
        return (EquivalenceFilter<T>) INSTANCE;
    }
    
    private AlwaysTrueFilter() {}
    
    @Override
    protected boolean doFilter(
            ScoredCandidate<T> input,
            T subject,
            ResultDescription desc,
            EquivToTelescopeResults equivToTelescopeResults
    ) {
        EquivToTelescopeComponent filterComponent = EquivToTelescopeComponent.create();
        filterComponent.setComponentName("Always True Filter");
        if (subject != null
                &&((Identified) subject).getId() != null) {
            filterComponent.addComponentResult(
                    ((Identified) input.candidate()).getId(),
                    "Always True"
            );
        }
        equivToTelescopeResults.addFilterResult(filterComponent);
        return true;
    }

}

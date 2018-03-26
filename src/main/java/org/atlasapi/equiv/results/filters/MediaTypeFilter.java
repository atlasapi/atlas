package org.atlasapi.equiv.results.filters;

import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.ScoredCandidate;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeComponent;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResults;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.MediaType;

import com.google.common.base.Objects;


public class MediaTypeFilter<T extends Content> extends AbstractEquivalenceFilter<T> {

    @Override
    protected boolean doFilter(
            ScoredCandidate<T> input,
            T subject,
            ResultDescription desc,
            EquivToTelescopeResults equivToTelescopeResults
    ) {
        EquivToTelescopeComponent filterComponent = EquivToTelescopeComponent.create();
        filterComponent.setComponentName("Media Type Filter");

        T equivalent = input.candidate();
        MediaType candMediaType = equivalent.getMediaType();
        MediaType subjMediaType = subject.getMediaType();
        
        boolean result = candMediaType == null 
            || subjMediaType == null 
            || Objects.equal(candMediaType, subjMediaType);
        
        if (!result) {
            desc.appendText("%s removed. %s != %s", 
                equivalent, candMediaType, subjMediaType);
            filterComponent.addComponentResult(
                    equivalent.getId(),
                    "Removed due to differing media types"
            );
        } else{
            filterComponent.addComponentResult(
                    equivalent.getId(),
                    "Went through."
            );
        }

        equivToTelescopeResults.addFilterResult(filterComponent);

        return result;
    }
}

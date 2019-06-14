package org.atlasapi.equiv.results.filters;

import com.google.common.base.Objects;
import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.ScoredCandidate;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeComponent;
import org.atlasapi.equiv.update.metadata.EquivToTelescopeResult;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.MediaType;


public class MediaTypeFilter<T extends Content> extends AbstractEquivalenceFilter<T> {

    @Override
    protected boolean doFilter(
            ScoredCandidate<T> input,
            T subject,
            ResultDescription desc,
            EquivToTelescopeResult equivToTelescopeResult
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

        equivToTelescopeResult.addFilterResult(filterComponent);

        return result;
    }
}

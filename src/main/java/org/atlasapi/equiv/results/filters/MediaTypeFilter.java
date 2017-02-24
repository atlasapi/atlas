package org.atlasapi.equiv.results.filters;

import org.atlasapi.equiv.results.description.ResultDescription;
import org.atlasapi.equiv.results.scores.ScoredCandidate;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.MediaType;

import com.google.common.base.Objects;


public class MediaTypeFilter<T extends Content> extends AbstractEquivalenceFilter<T> {

    @Override
    protected boolean doFilter(ScoredCandidate<T> input, T subject, ResultDescription desc) {
        T equivalent = input.candidate();
        MediaType candMediaType = equivalent.getMediaType();
        MediaType subjMediaType = subject.getMediaType();
        
        boolean result = candMediaType == null 
            || subjMediaType == null 
            || Objects.equal(candMediaType, subjMediaType);
        
        if (!result) {
            desc.appendText("%s removed. %s != %s", 
                equivalent, candMediaType, subjMediaType);
        }
        return result;
    }
}

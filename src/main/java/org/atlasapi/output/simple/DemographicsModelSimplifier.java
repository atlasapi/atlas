package org.atlasapi.output.simple;

import java.util.Set;

import org.atlasapi.application.v3.ApplicationConfiguration;
import org.atlasapi.media.entity.simple.Demographic;
import org.atlasapi.media.entity.simple.DemographicSegment;
import org.atlasapi.output.Annotation;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;


public class DemographicsModelSimplifier implements ModelSimplifier<org.atlasapi.media.entity.Demographic, Demographic> {

    @Override
    public Demographic simplify(org.atlasapi.media.entity.Demographic model,
            Set<Annotation> annotations, ApplicationConfiguration config) {
        Demographic demographic = new Demographic();
        demographic.setType(model.getType());
        demographic.setSegments(ImmutableList.copyOf(Iterables.transform(model.getSegments(), SIMPILFY_SEGMENT)));
        return demographic;
    }

    private static Function<org.atlasapi.media.entity.DemographicSegment, DemographicSegment> SIMPILFY_SEGMENT = 
            new Function<org.atlasapi.media.entity.DemographicSegment, DemographicSegment>() {

                @Override
                public DemographicSegment apply(org.atlasapi.media.entity.DemographicSegment input) {
                    return new DemographicSegment(input.getKey(), input.getLabel(), input.getValue());
                }
                
            };
}

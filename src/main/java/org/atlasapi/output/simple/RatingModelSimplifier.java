package org.atlasapi.output.simple;

import java.util.Set;

import org.atlasapi.application.v3.ApplicationConfiguration;
import org.atlasapi.media.entity.simple.Rating;
import org.atlasapi.output.Annotation;


public class RatingModelSimplifier implements ModelSimplifier<org.atlasapi.media.entity.Rating, Rating>{

    private final PublisherSimplifier publisherSimplifier = new PublisherSimplifier();
    
    @Override
    public Rating simplify(org.atlasapi.media.entity.Rating model, Set<Annotation> annotations,
            ApplicationConfiguration config) {
        Rating rating = new Rating();
        rating.setType(model.getType());
        rating.setValue(model.getValue());
        rating.setPublisherDetails(publisherSimplifier.simplify(model.getPublisher()));
        return rating;
    }

}

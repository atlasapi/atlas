package org.atlasapi.output.simple;

import java.util.Set;

import com.metabroadcast.applications.client.model.internal.Application;
import org.atlasapi.media.entity.simple.Rating;
import org.atlasapi.output.Annotation;


public class RatingModelSimplifier implements ModelSimplifier<org.atlasapi.media.entity.Rating, Rating>{

    private final PublisherSimplifier publisherSimplifier = new PublisherSimplifier();
    
    @Override
    public Rating simplify(
            org.atlasapi.media.entity.Rating model,
            Set<Annotation> annotations,
            Application application
    ) {
        Rating rating = new Rating();
        rating.setType(model.getType());
        rating.setValue(model.getValue());
        rating.setPublisherDetails(publisherSimplifier.simplify(model.getPublisher()));
        rating.setNumberOfVotes(model.getNumberOfVotes());
        return rating;
    }

}

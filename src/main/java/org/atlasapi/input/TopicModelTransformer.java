package org.atlasapi.input;

import com.metabroadcast.common.base.Maybe;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Topic;
import org.atlasapi.media.entity.simple.PublisherDetails;


public class TopicModelTransformer implements
        ModelTransformer<org.atlasapi.media.entity.simple.Topic, Topic> {

    @Override
    public Topic transform(org.atlasapi.media.entity.simple.Topic simple) {
        Topic output = new Topic(null);//we don't have an id yet.
        output.setPublisher(getPublisher(simple.getPublisher()));
        output.setNamespace(simple.getNamespace());
        output.setValue(simple.getValue());
        output.setType(Topic.Type.fromKey(simple.getType()));
        output.setTitle(simple.getTitle());
        output.setDescription(simple.getDescription());
        output.setImage(simple.getImage());
        output.setThumbnail(simple.getThumbnail());
        return output;
    }

    protected Publisher getPublisher(PublisherDetails pubDets) {
        if (pubDets == null || pubDets.getKey() == null) {
            throw new IllegalArgumentException("missing publisher");
        }
        Maybe<Publisher> possiblePublisher = Publisher.fromKey(pubDets.getKey());
        if (possiblePublisher.isNothing()) {
            throw new IllegalArgumentException("unknown publisher " + pubDets.getKey());
        }
        return possiblePublisher.requireValue();
    }
    
}

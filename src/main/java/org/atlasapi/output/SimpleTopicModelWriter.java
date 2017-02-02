package org.atlasapi.output;

import java.util.Set;

import org.atlasapi.application.v3.ApplicationConfiguration;
import org.atlasapi.media.entity.Topic;
import org.atlasapi.media.entity.simple.TopicQueryResult;
import org.atlasapi.output.simple.ModelSimplifier;
import org.atlasapi.persistence.content.ContentResolver;

public class SimpleTopicModelWriter extends TransformingModelWriter<Iterable<Topic>, TopicQueryResult> {

    private final ModelSimplifier<Topic, org.atlasapi.media.entity.simple.Topic> topicSimplifier;

    public SimpleTopicModelWriter(AtlasModelWriter<TopicQueryResult> delegate, ContentResolver contentResolver, ModelSimplifier<Topic, org.atlasapi.media.entity.simple.Topic> topicSimplifier) {
        super(delegate);
        this.topicSimplifier = topicSimplifier;
    }
    
    @Override
    protected TopicQueryResult transform(Iterable<Topic> fullTopics, Set<Annotation> annotations, ApplicationConfiguration config) {
        TopicQueryResult result = new TopicQueryResult();
        for (Topic fullTopic : fullTopics) {
            result.add(topicSimplifier.simplify(fullTopic, annotations, config));
        }
        return result;
    }

}

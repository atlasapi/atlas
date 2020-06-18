package org.atlasapi;

import java.util.Optional;

import javax.annotation.PostConstruct;

import org.atlasapi.media.entity.Topic;
import org.atlasapi.media.entity.TopicRef;
import org.atlasapi.persistence.topic.TopicStore;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ThematicLabels {

    // This is a quick hack to instantiate a static topic store from an autowired value. If this
    // class is to be improved in the future one should look into instantiating the topic store
    // properly. A possible problem of this implementation is if someone tries to use this class
    // before spring injects this class with a topic store value, which is unlikely in the way
    // this is currently used.
    @Autowired
    private TopicStore autowiredTopicStore;

    @PostConstruct
    private void init() {
        topicStore = this.autowiredTopicStore;
    }

    private static final Logger log = LoggerFactory.getLogger(ThematicLabels.class);
    private static final String LABEL_NAMESPACE = "gb:barb:thematicLabel";
    private static TopicStore topicStore;

    private static final LoadingCache<String, Optional<TopicRef>> thematicLabelCache = CacheBuilder
            .newBuilder()
            .build(new CacheLoader<String, Optional<TopicRef>>() {
                @Override
                public Optional<TopicRef> load(String value) {
                    return getTopicRefFromThematicValue(value);
                }
            });

    public static Optional<TopicRef> get(Value value) {
        return thematicLabelCache.getUnchecked(value.getValue());
    }

    private static Optional<TopicRef> getTopicRefFromThematicValue(String value) {

        Optional<Topic> topicOptional = topicStore.topicFor(
                LABEL_NAMESPACE,
                value
        ).toOptional();

        if (!topicOptional.isPresent()) {
            log.warn("Couldn't find thematic label for value {}.", value);
        }

        return topicOptional.map(topic -> new TopicRef(
                topic,
                0f,
                false,
                TopicRef.Relationship.ABOUT,
                0));
    }

    public enum Value {
        BBC_THREE("m0001"),
        ;

        String value;
        Value(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }
}

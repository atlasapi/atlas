package org.atlasapi;

import java.util.Optional;

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

    private static ThematicLabels thematicLabelsInstance = null;

    private static final Logger log = LoggerFactory.getLogger(ThematicLabels.class);
    private static final String LABEL_NAMESPACE = "gb:barb:thematicLabel";

    private @Autowired TopicStore topicStore;

    private final LoadingCache<String, Optional<TopicRef>> thematicLabelCache = CacheBuilder
            .newBuilder()
            .build(new CacheLoader<String, Optional<TopicRef>>() {
                @Override
                public Optional<TopicRef> load(String value) {
                    return getTopicRefFromThematicValue(value);
                }
            });

    public static synchronized ThematicLabels getInstance() {
        if (thematicLabelsInstance == null) {
            thematicLabelsInstance = new ThematicLabels();
        }
        return thematicLabelsInstance;
    }

    public Optional<TopicRef> get(Title title) {
        return thematicLabelCache.getUnchecked(title.getValue());
    }

    private Optional<TopicRef> getTopicRefFromThematicValue(String value) {

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

    public enum Title {
        BBC_THREE("m0001"),
        ;

        String value;
        Title(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }
}

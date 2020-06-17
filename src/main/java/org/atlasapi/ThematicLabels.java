package org.atlasapi;

import java.util.Map;
import java.util.Optional;

import javax.annotation.PostConstruct;

import org.atlasapi.media.entity.Topic;
import org.atlasapi.media.entity.TopicRef;
import org.atlasapi.persistence.topic.TopicStore;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ThematicLabels {

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

    private static final Map<String, String> thematicLabelTitleToValueRelations = ImmutableMap.of(
            "bbc_three", "m0001"
    );

    public static Optional<TopicRef> get(String title) {
        String thematicLabelValue = thematicLabelTitleToValueRelations.get(title);
        if (thematicLabelValue == null) {
            log.warn("A thematic label relation could not be found for title {}.", title);
            return Optional.empty();
        } else {
            return thematicLabelCache.getUnchecked(title);
        }
    }

    private static Optional<TopicRef> getTopicRefFromThematicValue(String value) {

        Optional<Topic> topicOptional = topicStore.topicFor(
                LABEL_NAMESPACE,
                value
        ).toOptional();

        if (!topicOptional.isPresent()) {
            log.warn(
                    "Couldn't find thematic label for value {}, the label will "
                    + "not attached to Nitro content.", value);
        }

        return topicOptional.map(topic -> new TopicRef(
                topic,
                0f,
                false,
                TopicRef.Relationship.ABOUT,
                0));
    }
}

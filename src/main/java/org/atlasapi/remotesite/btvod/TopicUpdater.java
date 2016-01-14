package org.atlasapi.remotesite.btvod;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.concurrent.ExecutionException;

import javax.annotation.Nullable;

import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Topic;
import org.atlasapi.media.entity.TopicRef;
import org.atlasapi.persistence.topic.TopicQueryResolver;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableSet;

public class TopicUpdater {

    private final TopicQueryResolver topicQueryResolver;
    private final ImmutableSet<Predicate<Topic>> shouldPropagatePredicates;
    private final LoadingCache<TopicRef, Topic> topicCache;

    public TopicUpdater(TopicQueryResolver topicQueryResolver,
            Iterable<Topic> topicsToPropagate, Iterable<String> namespacesToPropagate) {
        this.topicQueryResolver = checkNotNull(topicQueryResolver);
        this.shouldPropagatePredicates = getPredicates(
                checkNotNull(topicsToPropagate),
                checkNotNull(namespacesToPropagate)
        );
        this.topicCache = CacheBuilder.newBuilder()
                .maximumSize(10000)
                .build(new CacheLoader<TopicRef, Topic>() {
                    @Override public Topic load(TopicRef key) throws Exception {
                        return TopicUpdater.this
                                .topicQueryResolver.topicForId(key.getTopic()).requireValue();
                    }
                });
    }

    public void updateTopics(Content content, Iterable<TopicRef> topicRefs) {
        for (TopicRef topicRef : topicRefs) {
            Topic topic = topicFor(topicRef);

            boolean shouldPropagate = Predicates.or(shouldPropagatePredicates).apply(topic);
            if (shouldPropagate && !content.getTopicRefs().contains(topicRef)) {
                content.addTopicRef(topicRef);
            }
        }
    }

    private Topic topicFor(TopicRef topicRef) {
        try {
            return topicCache.get(topicRef);
        } catch (ExecutionException e) {
            throw Throwables.propagate(e);
        }
    }

    private ImmutableSet<Predicate<Topic>> getPredicates(Iterable<Topic> topicsToPropagate,
            Iterable<String> namespacesToPropagate) {
        ImmutableSet.Builder<Predicate<Topic>> predicates = ImmutableSet.builder();

        for (final Topic topic : topicsToPropagate) {
            predicates.add(getTopicMatchingPredicate(topic));
        }

        for (final String namespace : namespacesToPropagate) {
            predicates.add(getNamespaceMatchingPredicate(namespace));
        }

        return predicates.build();
    }

    private Predicate<Topic> getTopicMatchingPredicate(final Topic topic) {
        return new Predicate<Topic>() {

            @Override public boolean apply(@Nullable Topic input) {
                return input != null && topic.getId().equals(input.getId());
            }
        };
    }

    private Predicate<Topic> getNamespaceMatchingPredicate(final String namespace) {
        return new Predicate<Topic>() {

            @Override public boolean apply(@Nullable Topic input) {
                return input != null && namespace.equals(input.getNamespace());
            }
        };
    }
}

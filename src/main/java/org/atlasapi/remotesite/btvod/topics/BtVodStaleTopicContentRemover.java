package org.atlasapi.remotesite.btvod.topics;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import org.atlasapi.content.criteria.ContentQueryBuilder;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Topic;
import org.atlasapi.media.entity.TopicRef;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.topic.TopicContentLister;
import org.atlasapi.remotesite.btvod.BtVodContentListener;
import org.atlasapi.remotesite.btvod.model.BtVodEntry;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;


/**
 * Remove topics from content where the content should not longer
 * have that topic.
 * 
 * Some topics are generated from a feed which specifies the list of
 * content that should have the topic. Since this feed is a point-
 * in-time snapshot of the contents and not a change feed, once the
 * current data set has topics added, we must remove from content
 * where it's no longer valid.
 *
 */
public class BtVodStaleTopicContentRemover implements BtVodContentListener {

    private final TopicContentLister topicContentLister;
    private final ContentWriter contentWriter;
    private final Multimap<Long, String> topicToCurrentlyValidContentUris = ArrayListMultimap.create();
    private final Predicate<TopicRef> topicsOfInterest;
    private final Set<Long> topicIdsToMaintain;

    public BtVodStaleTopicContentRemover(Iterable<Topic> topicsToMaintain, 
            TopicContentLister topicContentLister, ContentWriter contentWriter) {
        this.topicIdsToMaintain = ImmutableSet.copyOf(Iterables.transform(topicsToMaintain, TOPIC_TO_TOPIC_ID));
        this.topicContentLister = checkNotNull(topicContentLister);
        this.contentWriter = checkNotNull(contentWriter);
        this.topicsOfInterest = topicsOfInterest(topicIdsToMaintain);
    }
    
    @Override
    public void beforeContent() {
        topicToCurrentlyValidContentUris.clear();
    }

    @Override
    public void onContent(Content content, BtVodEntry vodData) {
        for (TopicRef topicRef : Iterables.filter(content.getTopicRefs(), topicsOfInterest)) {
            topicToCurrentlyValidContentUris.put(topicRef.getTopic(), content.getCanonicalUri());
        }
    }

    @Override
    public void afterContent() {
        for (final Long topicId : topicIdsToMaintain) {
            Collection<String> urisForTopic = topicToCurrentlyValidContentUris.get(topicId);
            Iterator<Content> contents = topicContentLister.contentForTopic(topicId, ContentQueryBuilder.query().build());
            while (contents.hasNext()) {
                Content content = contents.next();
                if (!urisForTopic.contains(content.getCanonicalUri())) {
                    removeTopicFrom(content, topicId);
                    write(content);
                }
            }
        }
    }
    
    private void write(Content content) {
        if (content instanceof Container) {
            contentWriter.createOrUpdate((Container) content);
        } else if (content instanceof Item) {
            contentWriter.createOrUpdate((Item) content);
        } else {
            throw new IllegalArgumentException("Can't cope with content of type " + content.getClass().getName());
        }
    }

    private void removeTopicFrom(Content content, final Long topicId) {
        content.setTopicRefs(Iterables.filter(content.getTopicRefs(), new Predicate<TopicRef>() {

            @Override
            public boolean apply(TopicRef input) {
                return !input.getTopic().equals(topicId);
            }
            
        }));
    }

    private Predicate<TopicRef> topicsOfInterest(final Set<Long> topicIds) {
        return new Predicate<TopicRef>() {

            @Override
            public boolean apply(TopicRef input) {
                return topicIds.contains(input.getTopic());
            }
        };
    }
    
    private static Function<Topic, Long> TOPIC_TO_TOPIC_ID = new Function<Topic, Long>() {

        @Override
        public Long apply(Topic input) {
            return input.getId();
        }
    };
     
}

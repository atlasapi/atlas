package org.atlasapi.query.topic;

import java.util.Iterator;
import java.util.Set;

import org.atlasapi.content.criteria.ContentQuery;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.topic.TopicContentLister;

import com.google.common.collect.Iterators;

public class PublisherFilteringTopicContentLister implements TopicContentLister {
    
    private final TopicContentLister delegate;

    public PublisherFilteringTopicContentLister(TopicContentLister delegate) {
        this.delegate = delegate;
    }

    @Override
    public Iterator<Content> contentForTopic(Long topicId, ContentQuery contentQuery) {
        final Set<Publisher> includedPublishers = contentQuery.getApplication()
                .getConfiguration()
                .getEnabledReadSources();
        return Iterators.filter(delegate.contentForTopic(topicId, contentQuery),
                input -> includedPublishers.contains(input.getPublisher()));
    }
    
    

}

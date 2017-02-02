package org.atlasapi.query.topic;

import java.util.Set;

import org.atlasapi.content.criteria.ContentQuery;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Topic;
import org.atlasapi.persistence.topic.TopicQueryResolver;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.metabroadcast.common.base.Maybe;

public class PublisherFilteringTopicResolver implements TopicQueryResolver {

    private final TopicQueryResolver delegate;

    public PublisherFilteringTopicResolver(TopicQueryResolver delegate) {
        this.delegate = delegate;
    }
    
    @Override
    public Iterable<Topic> topicsFor(ContentQuery query) {
        final Set<Publisher> includedPublishers = query.getConfiguration().getEnabledSources();
        return query.getSelection().applyTo(Iterables.filter(delegate.topicsFor(query), new Predicate<Topic>() {
            @Override
            public boolean apply(Topic input) {
                return includedPublishers.contains(input.getPublisher());
            }
        }));
    }
    
    //TODO pass in ContentQuery, filter by publisher, selection etc...

    @Override
    public Maybe<Topic> topicForId(Long id) {
        return delegate.topicForId(id);
    }

    @Override
    public Iterable<Topic> topicsForIds(Iterable<Long> ids) {
        return delegate.topicsForIds(ids);
    }
}

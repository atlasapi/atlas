package org.atlasapi.remotesite.btvod;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Topic;
import org.atlasapi.media.entity.TopicRef;
import org.atlasapi.persistence.topic.TopicQueryResolver;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.ImmutableList;
import com.metabroadcast.common.base.Maybe;

@RunWith(MockitoJUnitRunner.class)
public class TopicUpdaterTest {

    private @Mock TopicQueryResolver topicQueryResolver;

    private TopicRef firstTopicRef;
    private TopicRef secondTopicRef;
    private TopicRef thirdTopicRef;

    private @Mock Content content;

    private TopicUpdater topicUpdater;

    @Before
    public void setUp() throws Exception {
        firstTopicRef = new TopicRef(1L, 1.0f, false, TopicRef.Relationship.ABOUT);
        secondTopicRef = new TopicRef(2L, 1.0f, false, TopicRef.Relationship.ABOUT);
        thirdTopicRef = new TopicRef(3L, 1.0f, false, TopicRef.Relationship.ABOUT);

        Topic firstTopic = new Topic(firstTopicRef.getTopic(), "nsA", "valA");
        Topic secondTopic = new Topic(secondTopicRef.getTopic(), "nsB", "valB");
        Topic thirdTopic = new Topic(thirdTopicRef.getTopic(), "nsC", "valC");

        when(topicQueryResolver.topicForId(firstTopicRef.getTopic()))
                .thenReturn(Maybe.just(firstTopic));
        when(topicQueryResolver.topicForId(secondTopicRef.getTopic()))
                .thenReturn(Maybe.just(secondTopic));
        when(topicQueryResolver.topicForId(thirdTopicRef.getTopic()))
                .thenReturn(Maybe.just(thirdTopic));

        topicUpdater = new TopicUpdater(
                topicQueryResolver,
                ImmutableList.of(firstTopic),
                ImmutableList.of(secondTopic.getNamespace())
        );
    }

    @Test
    public void testPropagateTopicsThatAreExplicitlyDefinedToBePropagated() throws Exception {
        when(content.getTopicRefs()).thenReturn(ImmutableList.<TopicRef>of());

        topicUpdater.updateTopics(content, ImmutableList.of(firstTopicRef));

        verify(content).addTopicRef(firstTopicRef);
    }

    @Test
    public void testPropagateTopicsWithNamespaceThatShouldBePropagated() throws Exception {
        when(content.getTopicRefs()).thenReturn(ImmutableList.<TopicRef>of());

        topicUpdater.updateTopics(content, ImmutableList.of(secondTopicRef));

        verify(content).addTopicRef(secondTopicRef);
    }

    @Test
    public void testDoNotPropagateTopicsThatAreNotDesired() throws Exception {
        when(content.getTopicRefs()).thenReturn(ImmutableList.<TopicRef>of());

        topicUpdater.updateTopics(content, ImmutableList.of(thirdTopicRef));

        verify(content, never()).addTopicRef(thirdTopicRef);
    }

    @Test
    public void testTopicCacheIsBeingUsed() throws Exception {
        when(content.getTopicRefs()).thenReturn(ImmutableList.<TopicRef>of());

        topicUpdater.updateTopics(content, ImmutableList.of(firstTopicRef));
        topicUpdater.updateTopics(content, ImmutableList.of(firstTopicRef));

        verify(topicQueryResolver, times(1)).topicForId(firstTopicRef.getTopic());
    }

    @Test
    public void testDoNotPropagateTopicIfItAlreadyExists() throws Exception {
        when(content.getTopicRefs()).thenReturn(ImmutableList.of(firstTopicRef));

        topicUpdater.updateTopics(content, ImmutableList.of(firstTopicRef));

        verify(content, never()).addTopicRef(firstTopicRef);
    }
}
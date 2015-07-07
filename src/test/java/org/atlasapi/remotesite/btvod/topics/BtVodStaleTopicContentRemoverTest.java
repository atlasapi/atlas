package org.atlasapi.remotesite.btvod.topics;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.atlasapi.content.criteria.ContentQueryBuilder;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Topic;
import org.atlasapi.media.entity.TopicRef;
import org.atlasapi.media.entity.testing.ComplexItemTestDataBuilder;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.topic.TopicContentLister;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;


@RunWith( MockitoJUnitRunner.class )
public class BtVodStaleTopicContentRemoverTest {
    
    private final Topic topicToMaintain = new Topic(1234L);

    private final TopicContentLister topicContentLister = mock(TopicContentLister.class);
    private final ContentWriter contentWriter = mock(ContentWriter.class);
    private final BtVodStaleTopicContentRemover underTest = new BtVodStaleTopicContentRemover(ImmutableSet.of(topicToMaintain), topicContentLister, contentWriter);
    
    @Test
    public void testRemovesStaleContentFromTopic() {
        TopicRef topicRef = new TopicRef(topicToMaintain.getId(), null, null, null);
        
        Content itemToRemoveTopicFrom = ComplexItemTestDataBuilder.complexItem().withUri("http://item1.org/").build();
        Content itemToRetainTopic = ComplexItemTestDataBuilder.complexItem().withUri("http://item2.org/").build();
        
        itemToRemoveTopicFrom.setTopicRefs(ImmutableSet.of(topicRef));
        itemToRetainTopic.setTopicRefs(ImmutableSet.of(topicRef));
        
        ArgumentCaptor<Item> itemCaptor = ArgumentCaptor.forClass(Item.class);
        
        when(topicContentLister.contentForTopic(topicToMaintain.getId(), ContentQueryBuilder.query().build()))
            .thenReturn(Sets.newHashSet(itemToRemoveTopicFrom, itemToRetainTopic).iterator());
        
        underTest.beforeContent();
        underTest.onContent(itemToRetainTopic, null);        
        underTest.afterContent();
        
        verify(contentWriter).createOrUpdate(itemCaptor.capture());
        
        Item item = Iterables.getOnlyElement(itemCaptor.getAllValues());
        assertThat(item.getCanonicalUri(), is(equalTo(itemToRemoveTopicFrom.getCanonicalUri())));
        assertTrue(item.getTopicRefs().isEmpty());
    }
}

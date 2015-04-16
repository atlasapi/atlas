package org.atlasapi.remotesite.getty;

import static org.hamcrest.Matchers.endsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.KeyPhrase;
import org.atlasapi.media.entity.MediaType;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.TopicRef;
import org.atlasapi.remotesite.knowledgemotion.topics.TopicGuesser;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

public class GettyContentExtractorTest {

    private final TopicGuesser topicGuesser = mock(TopicGuesser.class);
    private final GettyContentExtractor extractor = new GettyContentExtractor(topicGuesser);

    @Test
    public void testExtractItem() {
        Mockito.when(topicGuesser.guessTopics(Matchers.<Iterable<String>>any())).thenReturn(
                ImmutableSet.of(new TopicRef(Long.valueOf(0), 1.0f, false, TopicRef.Relationship.ABOUT)));
        
        VideoResponse video = new VideoResponse();
        video.setAssetId("id");
        video.setDateCreated("/Date(1406012400000+0700)/");
        video.setDescription("description");
        video.setDuration("00:01:01:10");
        video.setKeywords(ImmutableList.of("key"));
        video.setThumb("thumb");
        video.setTitle("title");
        video.setAspectRatios(ImmutableList.of("16:9"));
        
        Content content = extractor.extract(video);
        Item item = (Item) content;
        assertThat(item.getCanonicalUri(), endsWith("id"));
        assertEquals(item.getPublisher(), Publisher.KM_GETTY);
        assertEquals("description", item.getDescription());
        assertEquals("title", item.getTitle());
        assertEquals("title", item.getTitle());
        assertEquals(MediaType.VIDEO, content.getMediaType());
        assertEquals(new KeyPhrase("key", Publisher.KM_GETTY), Iterables.getOnlyElement(content.getKeyPhrases()));
        assertEquals("thumb", content.getThumbnail());
        assertEquals("thumb", content.getImage());
        assertEquals(Integer.valueOf(61), Iterables.getOnlyElement(item.getVersions()).getDuration());
        assertEquals("16:9", Iterables.getOnlyElement(
                Iterables.getOnlyElement(item.getVersions()).getManifestedAs())
                .getVideoAspectRatio());
    }
    
}

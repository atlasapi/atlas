package org.atlasapi.remotesite.getty;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.remotesite.ContentExtractor;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.mockito.stubbing.OngoingStubbing;

import com.google.common.collect.ImmutableList;

public class DefaultGettyDataHandlerTest {

    private static final ResolvedContent NOTHING_RESOLVED = ResolvedContent.builder().build();
    private static final VideoResponse EMPTY_ROW = emptyRow();
    
    private final ContentResolver resolver = mock(ContentResolver.class);
    private final ContentWriter writer = mock(ContentWriter.class);
    @SuppressWarnings("unchecked")
    private final ContentExtractor<VideoResponse, Content> extractor = mock(ContentExtractor.class);
    
    private final DefaultGettyDataHandler handler = new DefaultGettyDataHandler(resolver, writer, extractor);
    
    private static VideoResponse emptyRow() {
        VideoResponse video = new VideoResponse();
        video.setAssetId("id");
        video.setDateCreated("date");
        video.setDescription("description");
        video.setDuration("duration");
        video.setThumb("thumb");
        video.setTitle("title");
        return video;
    }
    
    @Test
    public void testWritingContent() {
        Item item1 = new Item("item1", "i", Publisher.KM_GETTY);
        item1.setTitle("title1");
        Item item2 = new Item("item2", "i", Publisher.KM_GETTY);
        item2.setTitle("title2");
        
        for(Content content : ImmutableList.<Content>of(item1, item2)) {
            OngoingStubbing<Content> stubbing = Mockito.<Content>when(extractor.extract(EMPTY_ROW));
            stubbing = stubbing.thenReturn(content);
            
            when(resolver.findByCanonicalUris(Matchers.<Iterable<String>>any())).thenReturn(NOTHING_RESOLVED);
            handler.handle(EMPTY_ROW);
        }

        verify(writer, times(1)).createOrUpdate(item1);
        verify(writer, times(1)).createOrUpdate(item2);
    }
    
}

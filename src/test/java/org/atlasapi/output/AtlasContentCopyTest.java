package org.atlasapi.output;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.atlasapi.application.v3.ApplicationConfiguration;
import org.atlasapi.media.entity.simple.Item;
import org.atlasapi.media.entity.simple.Playlist;
import org.atlasapi.media.entity.testing.BroadcastTestDataBuilder;
import org.atlasapi.media.entity.testing.ItemTestDataBuilder;
import org.atlasapi.media.entity.testing.LocationTestDataBuilder;
import org.atlasapi.media.entity.testing.PlaylistTestDataBuilder;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.servlet.StubHttpServletRequest;
import com.metabroadcast.common.servlet.StubHttpServletResponse;


public class AtlasContentCopyTest {
    private HttpServletRequest request = new StubHttpServletRequest();
    private HttpServletResponse response = new StubHttpServletResponse();

    @Test
    public void testCopyItem() throws Exception {
        JsonTranslator<Iterable<Item>> translator = new JsonTranslator<Iterable<Item>>();
        
        Item item = ItemTestDataBuilder.item()
            .withLocations(LocationTestDataBuilder.location().build())
            .withBroadcasts(BroadcastTestDataBuilder.broadcast().build())
            .withTags("tag1", "tag2")
            .withClips(ItemTestDataBuilder.item().build(), ItemTestDataBuilder.item().build())
            .withSameAs("item2", "item3")
            .withAliases("item4")
            .build();
        
        ByteArrayOutputStream outputStream1 = new ByteArrayOutputStream();
        
        translator.writeTo(request, response, ImmutableList.of(item), ImmutableSet.<Annotation>of(), ApplicationConfiguration.DEFAULT_CONFIGURATION);
        
        String itemOriginalString = outputStream1.toString(Charsets.UTF_8.name());
        outputStream1.close();
        
        ByteArrayOutputStream outputStream2 = new ByteArrayOutputStream();
        
        translator.writeTo(request, response, ImmutableList.of(item.copy()), ImmutableSet.<Annotation>of(), ApplicationConfiguration.DEFAULT_CONFIGURATION);
        
        String itemCopyString = outputStream2.toString(Charsets.UTF_8.name());
        outputStream2.close();
        
        assertEquals(itemOriginalString, itemCopyString);
    }
    
    @Test 
    public void testCopyPlaylist() throws Exception {
        JsonTranslator<Iterable<Playlist>> translator = new JsonTranslator<Iterable<Playlist>>();
        
        Item item1 = ItemTestDataBuilder.item()
        .withLocations(LocationTestDataBuilder.location().build())
        .withBroadcasts(BroadcastTestDataBuilder.broadcast().build())
        .withTags("tag1", "tag2")
        .withClips(ItemTestDataBuilder.item().build(), ItemTestDataBuilder.item().build())
        .withSameAs("item2", "item3")
        .withAliases("item4")
        .build();
        
        Item item2 = ItemTestDataBuilder.item()
        .withLocations(LocationTestDataBuilder.location().build())
        .withBroadcasts(BroadcastTestDataBuilder.broadcast().build())
        .withTags("tag1", "tag2")
        .withClips(ItemTestDataBuilder.item().build(), ItemTestDataBuilder.item().build())
        .withSameAs("item2", "item3")
        .withAliases("item4")
        .build();
        
        Playlist playlist = PlaylistTestDataBuilder.playlist()
            .withContent(item1, item2)
            .build();
        
        ByteArrayOutputStream outputStream1 = new ByteArrayOutputStream();
        
        translator.writeTo(request, response, ImmutableList.of(playlist), ImmutableSet.<Annotation>of(), ApplicationConfiguration.DEFAULT_CONFIGURATION);
        
        String playlistOriginalString = outputStream1.toString(Charsets.UTF_8.name());
        outputStream1.close();
        
        ByteArrayOutputStream outputStream2 = new ByteArrayOutputStream();
        
        translator.writeTo(request, response, ImmutableList.of(playlist.copy()), ImmutableSet.<Annotation>of(), ApplicationConfiguration.DEFAULT_CONFIGURATION);
        
        String playlistCopyString = outputStream2.toString(Charsets.UTF_8.name());
        outputStream2.close();
        
        System.out.println(playlistOriginalString);
        System.out.println(playlistCopyString);
        
        assertEquals(playlistOriginalString, playlistCopyString);
    }
}

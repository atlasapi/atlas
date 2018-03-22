package org.atlasapi.remotesite.channel4.pmlsd;

import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import com.google.common.io.Resources;
import com.metabroadcast.common.http.FixedResponseHttpClient;
import com.metabroadcast.common.http.SimpleHttpClient;
import com.metabroadcast.common.time.SystemClock;
import com.sun.syndication.feed.atom.Entry;
import com.sun.syndication.feed.atom.Feed;
import org.atlasapi.media.TransportSubType;
import org.atlasapi.media.TransportType;
import org.atlasapi.media.entity.Clip;
import org.atlasapi.media.entity.Encoding;
import org.atlasapi.media.entity.Location;
import org.atlasapi.media.entity.MediaType;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Version;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith( MockitoJUnitRunner.class )
public class C4BrandClipAdapterTest {
    
    SimpleHttpClient client = new FixedResponseHttpClient(
            ImmutableMap.<String, String>builder()  
            .put("https://pmlsc.channel4.com/pmlsd/hestons-mission-impossible/videos/all.atom", fileContentsFromResource("hestons-mission-impossible.atom"))
            .build());

    C4AtomApiClient apiClient = new C4AtomApiClient(
    		client,
    		"https://pmlsc.channel4.com/pmlsd/",
    		Optional.empty()
    );
    
    private final ContentFactory<Feed, Feed, Entry> contentFactory 
        = new SourceSpecificContentFactory<>(Publisher.C4_PMLSD, new C4AtomFeedUriExtractor());;

    
	private String fileContentsFromResource(String resourceName)  {
        try {
            return Files.toString(new File(Resources.getResource(getClass(), resourceName).getFile()), Charsets.UTF_8);
        } catch (IOException e) {
            Throwables.propagate(e);
        }
        return null;
    }
	
	@Test
	public void testExtractingClips() throws Exception {
		
	    C4LocationPolicyIds locationPolicyIds = C4LocationPolicyIds.builder().build();
		List<Clip> clips = new C4BrandClipAdapter(apiClient, Publisher.C4_PMLSD, new SystemClock(), contentFactory, locationPolicyIds)
		                        .fetch("http://pmlsc.channel4.com/pmlsd/hestons-mission-impossible");
		assertEquals(5, clips.size());
		
		for (Clip clip: clips) {
		    assertNotNull(clip.getCanonicalUri());
		    assertNotNull(clip.getTitle());
		    assertEquals(MediaType.VIDEO, clip.getMediaType());
		    assertFalse(clip.getIsLongForm());
		    
		    assertFalse(clip.getVersions().isEmpty());
		    for (Version version: clip.getVersions()) {
		        assertFalse(version.getManifestedAs().isEmpty());
		        for (Encoding encoding: version.getManifestedAs()) {
		            assertEquals(2, encoding.getAvailableAt().size());
		            
		            Location link = locationOfType(encoding.getAvailableAt(), TransportType.LINK);
		            assertNotNull(link);
		            assertNotNull(link.getUri());
		            
		            Location embed = locationOfType(encoding.getAvailableAt(), TransportType.EMBED);
		            assertNotNull(embed);
                    assertNull(embed.getUri());
                    assertNotNull(embed.getEmbedId());
                    assertTrue(embed.getEmbedCode().contains(embed.getEmbedId()));
                    assertEquals(TransportSubType.BRIGHTCOVE, embed.getTransportSubType());
		        }
		    }
		}
	}
	
	private Location locationOfType(Iterable<Location> locations, TransportType type) {
	    for (Location location: locations) {
	        if (type.equals(location.getTransportType())) {
	            return location;
	        }
	    }
	    return null;
	}
}

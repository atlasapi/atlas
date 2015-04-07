package org.atlasapi.remotesite.itv;

import java.util.List;

import junit.framework.TestCase;

import org.atlasapi.media.TransportType;
import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Encoding;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Location;
import org.atlasapi.media.entity.Version;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.remotesite.SiteSpecificAdapter;
import org.junit.Test;

import com.google.common.collect.Lists;

public class ItvMercuryBrandAdapterTest extends TestCase {
    
    private final List<Item> items = Lists.newArrayList();
    private final ContentWriter itemStoringWriter = new ContentWriter() {
        
        
        @Override
        public void createOrUpdate(Container container) {
        }
        
        @Override
        public Item createOrUpdate(Item item) {
            items.add(item);
            return item;
        }
    };
    
    private final SiteSpecificAdapter<Brand> adapter = new ItvMercuryBrandAdapter(itemStoringWriter);

    @Test
    public void testShouldGetBrand() throws Exception {
        String uri = "http://www.itv.com/itvplayer/video/?Filter=Emmerdale";
        Brand brand = adapter.fetch(uri);
        assertNotNull(brand);
        
        assertEquals(uri, brand.getCanonicalUri());
        assertEquals("itv:Emmerdale", brand.getCurie());
//        assertFalse(brand.getGenres().isEmpty());
        assertNotNull(brand.getTitle());
        assertNotNull(brand.getDescription());
        assertFalse(brand.getChildRefs().isEmpty());
        
        for (Item item: items) {
            assertNotNull(item.getTitle());
            assertNotNull(item.getDescription());
            //assertFalse(item.getGenres().isEmpty());
            assertFalse(item.getVersions().isEmpty());
            
            for (Version version: item.getVersions()) {
                assertFalse(version.getBroadcasts().isEmpty());
                assertFalse(version.getManifestedAs().isEmpty());
                
                for (Broadcast broadcast: version.getBroadcasts()) {
                    assertNotNull(broadcast.getBroadcastOn());
                    assertNotNull(broadcast.getTransmissionTime());
                    assertNotNull(broadcast.getTransmissionEndTime());
                }
                
                for (Encoding encoding: version.getManifestedAs()) {
                    assertFalse(encoding.getAvailableAt().isEmpty());
                    
                    for (Location location: encoding.getAvailableAt()) {
                        assertNotNull(location.getUri());
                        assertNotNull(location.getPolicy());
                        assertEquals(TransportType.LINK, location.getTransportType());
                    }
                }
            }
        }
    }

    @Test
    public void testShouldBeAbleToFetch() {
        assertTrue(adapter.canFetch("http://www.itv.com/itvplayer/video/?Filter=...Do%20the%20Funniest%20Things"));
        assertFalse(adapter.canFetch("http://www.itv.com/itvplayer/video/?Filter=1234"));
    }
}

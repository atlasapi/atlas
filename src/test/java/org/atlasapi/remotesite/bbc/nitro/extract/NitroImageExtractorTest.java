package org.atlasapi.remotesite.bbc.nitro.extract;

import static org.junit.Assert.assertEquals;

import org.atlasapi.media.entity.Image;
import org.junit.Test;

import com.metabroadcast.atlas.glycerin.model.Brand;

public class NitroImageExtractorTest {

    @Test
    public void testImageExtraction() {
        NitroImageExtractor extractor = new NitroImageExtractor(1024, 576);
        Brand.Images.Image source = new Brand.Images.Image();
        source.setTemplateUrl("hostname/image_$recipe");

        Image extracted = extractor.extract(source);

        assertEquals("http://hostname/image_1024x576", extracted.getCanonicalUri());
        assertEquals(1024, (int) extracted.getWidth());
        assertEquals(576, (int) extracted.getHeight());
    }

}

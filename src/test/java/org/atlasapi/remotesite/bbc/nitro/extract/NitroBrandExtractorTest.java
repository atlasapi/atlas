package org.atlasapi.remotesite.bbc.nitro.extract;

import org.atlasapi.media.entity.MediaType;
import org.junit.Assert;
import org.junit.Test;

import com.metabroadcast.atlas.glycerin.model.Brand;
import com.metabroadcast.atlas.glycerin.model.Brand.MasterBrand;
import com.metabroadcast.common.time.SystemClock;

public class NitroBrandExtractorTest {

    private NitroBrandExtractor extractor = new NitroBrandExtractor(new SystemClock());

    @Test
    public void testMediaTypeIsSetCorrectly() {
        org.atlasapi.media.entity.Brand extractedAudio = extractor.extract(audioBrand());
        org.atlasapi.media.entity.Brand extractedVideo = extractor.extract(videoBrand());

        Assert.assertEquals(MediaType.AUDIO, extractedAudio.getMediaType());
        Assert.assertEquals(MediaType.VIDEO, extractedVideo.getMediaType());
    }

    private Brand audioBrand() {
        Brand brand = new Brand();
        brand.setPid("b04t74m8");

        MasterBrand masterBrand = new MasterBrand();
        masterBrand.setMid("bbc_radio_four_extra");
        masterBrand.setHref("/nitro/api/master_brands?mid=bbc_radio_four_extra");

        brand.setMasterBrand(masterBrand);

        return brand;
    }

    private Brand videoBrand() {
        Brand brand = new Brand();
        brand.setPid("b04g6rhb");

        MasterBrand masterBrand = new MasterBrand();
        masterBrand.setMid("bbc_two");
        masterBrand.setHref("/nitro/api/master_brands?mid=bbc_two");

        brand.setMasterBrand(masterBrand);

        return brand;
    }

}

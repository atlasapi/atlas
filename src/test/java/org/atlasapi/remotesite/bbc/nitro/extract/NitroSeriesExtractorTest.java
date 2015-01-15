package org.atlasapi.remotesite.bbc.nitro.extract;

import org.atlasapi.media.entity.MediaType;
import org.junit.Assert;
import org.junit.Test;

import com.metabroadcast.atlas.glycerin.model.Brand;
import com.metabroadcast.atlas.glycerin.model.Series;
import com.metabroadcast.common.time.SystemClock;

public class NitroSeriesExtractorTest {

    private NitroSeriesExtractor extractor = new NitroSeriesExtractor(new SystemClock());

    @Test
    public void testMediaTypeIsSetCorrectly() {
        org.atlasapi.media.entity.Series extractedAudio = extractor.extract(audioSeries());
        org.atlasapi.media.entity.Series extractedVideo = extractor.extract(videoSeries());

        Assert.assertEquals(MediaType.AUDIO, extractedAudio.getMediaType());
        Assert.assertEquals(MediaType.VIDEO, extractedVideo.getMediaType());
    }

    private Series audioSeries() {
        Series series = new Series();
        series.setPid("b04t74m8");

        Brand.MasterBrand masterBrand = new Brand.MasterBrand();
        masterBrand.setMid("bbc_radio_four_extra");
        masterBrand.setHref("/nitro/api/master_brands?mid=bbc_radio_four_extra");

        series.setMasterBrand(masterBrand);

        return series;
    }

    private Series videoSeries() {
        Series series = new Series();
        series.setPid("b04g6rhb");

        Brand.MasterBrand masterBrand = new Brand.MasterBrand();
        masterBrand.setMid("bbc_two");
        masterBrand.setHref("/nitro/api/master_brands?mid=bbc_two");

        series.setMasterBrand(masterBrand);

        return series;
    }

}

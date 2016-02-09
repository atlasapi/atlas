package org.atlasapi.remotesite.bbc.nitro.extract;

import com.google.common.collect.Iterables;
import org.atlasapi.media.entity.Image;
import org.atlasapi.media.entity.ImageType;
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

    @Test
    public void testGenericImageExtraction() {
        org.atlasapi.media.entity.Series extractedGenericImage = extractor.extract(seriesWithGenericImage());
        Image genericImage = Iterables.getOnlyElement(extractedGenericImage.getImages());

        Assert.assertEquals("http://ichef.bbci.co.uk/images/ic/1024x576/p028s846.png", extractedGenericImage.getImage());
        Assert.assertEquals(ImageType.GENERIC_IMAGE_CONTENT_ORIGINATOR, genericImage.getType());
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

    private Series seriesWithGenericImage() {
        Series series = new Series();
        series.setPid("b04t74m8");

        Brand.Image image1 = new Brand.Image();
        image1.setTemplateUrl("http://ichef.bbci.co.uk/images/ic/1024x576/p028s846.png");

        Brand.Images.Image image = new Brand.Images.Image();
        image.setTemplateUrl("http://ichef.bbci.co.uk/images/ic/1024x576/p028s846.png");
        Brand.Images images = new Brand.Images();
        images.setImage(image);

        series.setImage(image1);
        series.setImages(images);

        Brand.MasterBrand masterBrand = new Brand.MasterBrand();
        masterBrand.setMid("bbc_radio_four_extra");
        masterBrand.setHref("/nitro/api/master_brands?mid=bbc_radio_four_extra");

        series.setMasterBrand(masterBrand);

        return series;
    }

}

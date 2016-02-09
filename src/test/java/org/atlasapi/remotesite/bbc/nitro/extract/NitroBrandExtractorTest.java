package org.atlasapi.remotesite.bbc.nitro.extract;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.atlasapi.media.entity.Image;
import org.atlasapi.media.entity.ImageType;
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

    @Test
    public void testGenericImageExtraction() {
        org.atlasapi.media.entity.Brand extractedGenericImage = extractor.extract(brandWithGenericImage());
        Image genericImage = Iterables.getOnlyElement(extractedGenericImage.getImages());

        Assert.assertEquals("http://ichef.bbci.co.uk/images/ic/1024x576/p028s846.png", extractedGenericImage.getImage());
        Assert.assertEquals(ImageType.GENERIC_IMAGE_CONTENT_ORIGINATOR, genericImage.getType());
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

    private Brand brandWithGenericImage() {
        Brand brand = new Brand();
        brand.setPid("b04t74m8");

        Brand.Image image1 = new Brand.Image();
        image1.setTemplateUrl("http://ichef.bbci.co.uk/images/ic/1024x576/p028s846.png");

        Brand.Images.Image image = new Brand.Images.Image();
        image.setTemplateUrl("http://ichef.bbci.co.uk/images/ic/1024x576/p028s846.png");
        Brand.Images images = new Brand.Images();
        images.setImage(image);

        brand.setImage(image1);
        brand.setImages(images);

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

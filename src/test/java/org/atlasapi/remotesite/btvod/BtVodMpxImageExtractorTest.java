package org.atlasapi.remotesite.btvod;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import org.atlasapi.media.entity.Image;
import org.atlasapi.media.entity.ImageType;
import org.atlasapi.remotesite.btvod.model.BtVodEntry;
import org.atlasapi.remotesite.btvod.model.BtVodImage;
import org.atlasapi.remotesite.btvod.model.BtVodPlproductImages;
import org.junit.Test;

import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class BtVodMpxImageExtractorTest {

    private static final String BASE_URL = "https://example.com/images/";

    BtVodMpxImageExtractor imageExtractor = new BtVodMpxImageExtractor(BASE_URL);


    @Test
    public void testExtractImages() throws Exception {
        BtVodEntry entry = new BtVodEntry();
        
        BtVodImage packShotImage1 = new BtVodImage();
        packShotImage1.setPlproduct$mediaFileId("mediaFileId1");
        packShotImage1.setPlproduct$height(1);
        packShotImage1.setPlproduct$width(2);
        packShotImage1.setPlproduct$url("imageUrl1");

        BtVodImage packShotImage2 = new BtVodImage();
        packShotImage2.setPlproduct$mediaFileId("mediaFileId2");
        packShotImage2.setPlproduct$height(3);
        packShotImage2.setPlproduct$width(4);
        packShotImage2.setPlproduct$url("imageUrl2");

        BtVodImage backgroundImage1 = new BtVodImage();
        backgroundImage1.setPlproduct$mediaFileId("mediaFileId3");
        backgroundImage1.setPlproduct$height(5);
        backgroundImage1.setPlproduct$width(6);
        backgroundImage1.setPlproduct$url("imageUrl3");

        BtVodImage backgroundImage2 = new BtVodImage();
        backgroundImage2.setPlproduct$mediaFileId("mediaFileId4");
        backgroundImage2.setPlproduct$height(7);
        backgroundImage2.setPlproduct$width(8);
        backgroundImage2.setPlproduct$url("imageUrl4");

        BtVodImage packShotImageSd = new BtVodImage();
        packShotImageSd.setPlproduct$mediaFileId("mediaFileIdSd");
        packShotImageSd.setPlproduct$height(9);
        packShotImageSd.setPlproduct$width(10);
        packShotImageSd.setPlproduct$url("imageUrlSd");

        BtVodPlproductImages images = new BtVodPlproductImages();
        images.setSinglePackshotImages(ImmutableList.of(packShotImageSd));
        images.setSinglePackshotImagesHd(ImmutableList.of(packShotImage1, packShotImage2));
        images.setBackgroundImages(ImmutableList.of(backgroundImage1, backgroundImage2));

        entry.setProductImages(images);
        Set<Image> extractedImages = imageExtractor.imagesFor(entry);

        assertThat(extractedImages.size(), is(3));

        assertThat(
                Iterables.any(
                        extractedImages,
                        predicateFor(BASE_URL + "imageUrl1", 1, 2, ImageType.PRIMARY)
                ),
                is(true)
        );

        assertThat(
                Iterables.any(
                        extractedImages,
                        predicateFor(BASE_URL + "imageUrl2", 3, 4, ImageType.PRIMARY)
                ),
                is(true)
        );

        assertThat(
                Iterables.any(
                        extractedImages,
                        predicateFor(BASE_URL + "imageUrlSd", 9, 10, ImageType.PRIMARY)
                ),
                is(true)
        );

    }

    private Predicate<Image> predicateFor(final String url, final Integer height, final Integer width, final ImageType type) {
        return new Predicate<Image>() {
            @Override
            public boolean apply(Image input) {
                return input.getCanonicalUri().equals(url)
                        && input.getHeight().equals(height)
                        && input.getWidth().equals(width)
                        && input.getType().equals(type);
            }
        };
    }
}
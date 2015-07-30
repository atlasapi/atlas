package org.atlasapi.remotesite.btvod;

import com.google.common.collect.ImmutableSet;

import org.atlasapi.media.entity.Image;
import org.atlasapi.media.entity.ImageType;
import org.atlasapi.remotesite.btvod.model.BtVodEntry;
import org.atlasapi.remotesite.btvod.model.BtVodImage;
import org.atlasapi.remotesite.btvod.model.BtVodPlproductImages;

import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

public class BtVodMpxImageExtractor implements ImageExtractor {

    private final String baseUrl;

    public BtVodMpxImageExtractor(String baseUrl) {
        this.baseUrl = checkNotNull(baseUrl);
    }

    @Override
    public Set<Image> imagesFor(BtVodEntry entry) {
        BtVodPlproductImages btVodPlproductImages = entry.getProductImages();
        ImmutableSet.Builder<Image> extractedImages = ImmutableSet.builder();

        for (BtVodImage packshotImage : getPackshotImages(entry)) {
            extractedImages.add(
                    buildImage(packshotImage, ImageType.PRIMARY, true)
            );
        }

        for (BtVodImage backgroundImage : btVodPlproductImages.getBackgroundImages()) {
            extractedImages.add(
                    buildImage(backgroundImage, ImageType.ADDITIONAL, false)
            );
        }

        return extractedImages.build();
    }

    private List<BtVodImage> getPackshotImages(BtVodEntry entry) {
        
        BtVodPlproductImages images = entry.getProductImages();
        if (BrandUriExtractor.SERIES_TYPE.equals(entry.getProductType())) {
            if (!images.getHdPackshotDoubleImages().isEmpty()) {
                return images.getHdPackshotDoubleImages();
            } else {
                return images.getPackshotDoubleImages();
            }
        } else {
            if (!images.getHdPackshotImages().isEmpty()) {
                return images.getHdPackshotImages();
            } else {
                return images.getPackshotImages();
            }
        }
    }
    
    private Image buildImage(BtVodImage btVodImage, ImageType imageType, boolean hasTitleArt) {
        return Image.builder(uriFor(btVodImage))
                .withHeight(btVodImage.getPlproduct$height())
                .withWidth(btVodImage.getPlproduct$width())
                .withType(imageType)
                .withHasTitleArt(hasTitleArt)
                .build();
    }

    private String uriFor(BtVodImage image) {
        return String.format(
                "%s%s",
                baseUrl,
                image.getPlproduct$url()
        );
    }

    @Override
    public void start() {
        
    }
}

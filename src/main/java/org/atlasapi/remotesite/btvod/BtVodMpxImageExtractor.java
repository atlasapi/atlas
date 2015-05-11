package org.atlasapi.remotesite.btvod;

import com.google.common.collect.ImmutableSet;
import org.atlasapi.media.entity.Image;
import org.atlasapi.media.entity.ImageType;
import org.atlasapi.remotesite.btvod.model.BtVodImage;
import org.atlasapi.remotesite.btvod.model.BtVodPlproduct$images;

import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

public class BtVodMpxImageExtractor implements ImageExtractor {

    private final String baseUrl;

    public BtVodMpxImageExtractor(String baseUrl) {
        this.baseUrl = checkNotNull(baseUrl);
    }

    @Override
    public Set<Image> extractImages(BtVodPlproduct$images btVodPlproduct$images) {
        ImmutableSet.Builder<Image> extractedImages = ImmutableSet.builder();

        for (BtVodImage packshotImage : btVodPlproduct$images.getPackshotImages()) {
            extractedImages.add(
                    buildImage(packshotImage, ImageType.PRIMARY)
            );
        }

        for (BtVodImage backgroundImage : btVodPlproduct$images.getBackgroundImages()) {
            extractedImages.add(
                    buildImage(backgroundImage, ImageType.ADDITIONAL)
            );
        }


        return extractedImages.build();
    }

    private Image buildImage(BtVodImage btVodImage, ImageType imageType) {
        return Image.builder(uriFor(btVodImage))
                .withHeight(btVodImage.getPlproduct$height())
                .withWidth(btVodImage.getPlproduct$width())
                .withType(imageType)
                .build();
    }

    private String uriFor(BtVodImage image) {
        return String.format(
                "%s%s",
                baseUrl,
                image.getPlproduct$url()
        );
    }
}

package org.atlasapi.remotesite.btvod;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import org.atlasapi.media.entity.Image;
import org.atlasapi.media.entity.ImageType;
import org.atlasapi.remotesite.btvod.model.BtVodEntry;
import org.atlasapi.remotesite.btvod.model.BtVodImage;
import org.atlasapi.remotesite.btvod.model.BtVodPlproductImages;

import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

public class BtVodMpxImageExtractor implements ImageExtractor {

    private final String baseUrl;

    public BtVodMpxImageExtractor(String baseUrl) {
        this.baseUrl = checkNotNull(baseUrl);
    }

    @Override
    public Set<Image> imagesFor(BtVodEntry entry) {
        return ImmutableSet.copyOf(getPackshotImages(entry));
    }

    @SuppressWarnings("unchecked")
    private Iterable<Image> getPackshotImages(BtVodEntry entry) {
        
        BtVodPlproductImages images = entry.getProductImages();
        
        return Iterables.concat(
                        Iterables.transform(images.getDoublePackshotImagesHd(), toImage(false, ImageType.PRIMARY)),
                        Iterables.transform(images.getSinglePackshotImagesHd(), toImage(false, ImageType.PRIMARY)),
                        Iterables.transform(images.getBackgroundImages(), toImage(false, ImageType.PRIMARY)),
                        Iterables.transform(images.getSinglePackshotImages(), toImage(true, ImageType.PRIMARY)),
                        Iterables.transform(images.getDoublePackshotImages(), toImage(true, ImageType.PRIMARY)),
                        Iterables.transform(images.getDoublePackshotHiresImages(), toImage(true, ImageType.PRIMARY)),
                        Iterables.transform(images.getSinglePackshotHiresImages(), toImage(true, ImageType.PRIMARY))
                    );
    }

    
    private Function<BtVodImage, Image> toImage(final boolean hasTitleArt, final ImageType imageType) {
        return new Function<BtVodImage, Image>() {

            @Override
            public Image apply(BtVodImage input) {
                return Image.builder(uriFor(input))
                        .withHeight(input.getPlproduct$height())
                        .withWidth(input.getPlproduct$width())
                        .withType(imageType)
                        .withHasTitleArt(hasTitleArt)
                        .build();
            }
            
        };
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

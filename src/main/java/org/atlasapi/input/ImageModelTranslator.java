package org.atlasapi.input;

import com.metabroadcast.common.media.MimeType;
import org.atlasapi.media.entity.Image;
import org.atlasapi.media.entity.ImageAspectRatio;
import org.atlasapi.media.entity.ImageColor;
import org.atlasapi.media.entity.ImageTheme;
import org.joda.time.DateTime;

public class ImageModelTranslator implements ModelTransformer<org.atlasapi.media.entity.simple.Image, Image> {

    public static ImageModelTranslator create() {
        return new ImageModelTranslator();
    }

    @Override
    public Image transform(org.atlasapi.media.entity.simple.Image simple) {
        Image.Builder complex = Image.builder(simple.getUri());

        if (simple.getColor() != null) {
            complex.withColor(ImageColor.valueOf(simple.getColor().toUpperCase()));
        }
        if (simple.getTheme() != null) {
            complex.withTheme(ImageTheme.valueOf(simple.getTheme().toUpperCase()));
        }
        if (simple.getWidth() != null) {
            complex.withWidth(simple.getWidth());
        }
        if (simple.getHeight() != null) {
            complex.withHeight(simple.getHeight());
        }
        if (simple.getAspectRatio() != null) {
            if (ImageAspectRatio.FOUR_BY_THREE.getName().equals(simple.getAspectRatio())) {
                complex.withAspectRatio(ImageAspectRatio.FOUR_BY_THREE);
            } else if (ImageAspectRatio.SIXTEEN_BY_NINE.getName().equals(simple.getAspectRatio())) {
                complex.withAspectRatio(ImageAspectRatio.SIXTEEN_BY_NINE);
            }
        }
        if (simple.getMimeType() != null) {
            complex.withMimeType(MimeType.fromString(simple.getMimeType()));
        }
        if (simple.getAvailabilityStart() != null) {
            complex.withAvailabilityStart(new DateTime(simple.getAvailabilityStart()));
        }
        if (simple.getAvailabilityEnd() != null) {
            complex.withAvailabilityEnd(new DateTime(simple.getAvailabilityEnd()));
        }

        return complex.build();
    }
}

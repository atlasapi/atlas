package org.atlasapi.input;

import org.atlasapi.media.entity.Image;
import org.atlasapi.media.entity.ImageAspectRatio;
import org.atlasapi.media.entity.ImageColor;
import org.atlasapi.media.entity.ImageTheme;

import com.metabroadcast.common.media.MimeType;

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
            complex.withTheme(ImageTheme.valueOf(simple.getTheme()));
        }
        if (simple.getWidth() != null) {
            complex.withWidth(simple.getWidth());
        }
        if (simple.getHeight() != null) {
            complex.withHeight(simple.getHeight());
        }
        if (simple.getAspectRatio() != null) {
            complex.withAspectRatio(ImageAspectRatio.valueOf(simple.getAspectRatio()));
        }
        if (simple.getMimeType() != null) {
            complex.withMimeType(MimeType.valueOf(simple.getMimeType()));
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

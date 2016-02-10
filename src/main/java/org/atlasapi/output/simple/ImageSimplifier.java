package org.atlasapi.output.simple;

import java.util.Set;

import org.atlasapi.application.v3.ApplicationConfiguration;
import org.atlasapi.media.entity.Image;
import org.atlasapi.media.entity.ImageType;
import org.atlasapi.output.Annotation;


public class ImageSimplifier extends IdentifiedModelSimplifier<Image, org.atlasapi.media.entity.simple.Image> {

    @Override
    public org.atlasapi.media.entity.simple.Image simplify(Image image,
            Set<Annotation> annotations, ApplicationConfiguration config) {
        if (image == null) {
            return new org.atlasapi.media.entity.simple.Image();
        }
        org.atlasapi.media.entity.simple.Image simpleImage = new org.atlasapi.media.entity.simple.Image(image.getCanonicalUri());
        if (image.getType() != null) {
            simpleImage.setImageType(image.getType().getName());
        }

        if (image.getColor() != null) {
            simpleImage.setColor(image.getColor().getName());
        }
        if (image.getTheme() != null) {
            simpleImage.setTheme(image.getTheme().getName());
        }
        if (image.getWidth() != null) {
            simpleImage.setWidth(image.getWidth());
        }
        if (image.getHeight() != null) {
            simpleImage.setHeight(image.getHeight());
        }
        if (image.getAspectRatio() != null) {
            simpleImage.setAspectRatio(image.getAspectRatio().getName());
        }
        if (image.getMimeType() != null) {
            simpleImage.setMimeType(image.getMimeType().toString());
        }
        if (image.getAvailabilityStart() != null) {
            simpleImage.setAvailabilityStart(image.getAvailabilityStart().toDate());
        }
        if (image.getAvailabilityEnd() != null) {
            simpleImage.setAvailabilityEnd(image.getAvailabilityEnd().toDate());
        }
        
        return simpleImage;
    }

}

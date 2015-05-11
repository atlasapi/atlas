package org.atlasapi.remotesite.btvod;

import org.atlasapi.media.entity.Image;
import org.atlasapi.remotesite.btvod.model.BtVodPlproduct$images;

import java.util.Set;

public interface ImageExtractor {

    Set<Image> extractImages(BtVodPlproduct$images btVodPlproduct$images);
}

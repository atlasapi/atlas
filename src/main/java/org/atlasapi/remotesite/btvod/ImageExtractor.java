package org.atlasapi.remotesite.btvod;

import org.atlasapi.media.entity.Image;
import org.atlasapi.remotesite.btvod.model.BtVodPlproductImages;

import java.util.Set;

public interface ImageExtractor {

    Set<Image> extractImages(BtVodPlproductImages btVodPlproductImages);
}

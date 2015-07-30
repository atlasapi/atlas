package org.atlasapi.remotesite.btvod;

import org.atlasapi.media.entity.Image;
import org.atlasapi.remotesite.btvod.model.BtVodEntry;

import java.util.Set;

public interface ImageExtractor {

    Set<Image> brandImagesFor(BtVodEntry btVodPlproductImages);
    void start();
}

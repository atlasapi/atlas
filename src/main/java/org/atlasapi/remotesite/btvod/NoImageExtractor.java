package org.atlasapi.remotesite.btvod;

import java.util.Set;

import org.atlasapi.media.entity.Image;
import org.atlasapi.remotesite.btvod.model.BtVodEntry;

import com.google.common.collect.ImmutableSet;


public class NoImageExtractor implements ImageExtractor, BrandImageExtractor {

    @Override
    public Set<Image> imagesFor(BtVodEntry btVodPlproductImages) {
        return ImmutableSet.of();
    }

    @Override
    public void start() {
        
    }

    @Override
    public boolean process(BtVodEntry row) {
        return true;
    }

    @Override
    public Void getResult() {
        return null;
    }

}

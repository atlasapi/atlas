package org.atlasapi.remotesite.btvod;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Map;
import java.util.Set;

import org.atlasapi.media.entity.Image;
import org.atlasapi.remotesite.btvod.model.BtVodEntry;

import com.google.api.client.util.Maps;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extract brand images from MPX feed.
 * 
 * Images aren't provided at the brand level, however. We therefore
 * locate images from the first series, in the case of a brand with
 * series, or the last episode in the case of brands without them.
 * 
 * To do this in an efficient manner during ingest, and avoid writing a
 * brand many times, users of this class are expected to pre-process the
 * feed so the correct images for each brand can be computed before
 * brands are written.
 *
 */
public class DerivingFromSeriesBrandImageExtractor implements BrandImageExtractor, BtVodDataProcessor<Void> {

    private static final Logger log = LoggerFactory.getLogger(DerivingFromSeriesBrandImageExtractor.class);
    private final Map<String, BrandImages> bestImages = Maps.newHashMap(); 
    private final BrandUriExtractor brandUriExtractor;
    private final BtVodSeriesUriExtractor seriesUriExtractor;
    private final ImageExtractor baseImageExtractor;

    public DerivingFromSeriesBrandImageExtractor(
            BrandUriExtractor brandUriExtractor,
            BtVodSeriesUriExtractor seriesUriExtractor,
            ImageExtractor baseImageExtractor
    ) {
        this.brandUriExtractor = checkNotNull(brandUriExtractor);
        this.seriesUriExtractor = checkNotNull(seriesUriExtractor);
        this.baseImageExtractor = checkNotNull(baseImageExtractor);
    }
    
    @Override
    public Set<Image> imagesFor(BtVodEntry entry) {
        Optional<String> brandUri = brandUriExtractor.extractBrandUri(entry);
        if (!brandUri.isPresent()) {
            return ImmutableSet.of();
        }
        
        BrandImages currentBestImages = bestImages.get(brandUri.get());
        
        if (currentBestImages != null) {
            return currentBestImages.images;
        }
        
        return ImmutableSet.of();
    }

    @Override
    public boolean process(BtVodEntry entry) {
        Optional<String> brandUri = brandUriExtractor.extractBrandUri(entry);
        
        if (!BrandUriExtractor.SERIES_TYPE.equals(entry.getProductType())
                && !BtVodItemExtractor.EPISODE_TYPE.equals(entry.getProductType())) {
            return true;
        }
        
        Integer seriesNumber = seriesUriExtractor.extractSeriesNumber(entry).orNull();
        Integer episodeNumber = null;
        if (BtVodItemExtractor.EPISODE_TYPE.equals(entry.getProductType())) {
            episodeNumber = BtVodItemExtractor.extractEpisodeNumber(entry);
        }
        if (seriesNumber != null || episodeNumber != null) {
            if(!brandUri.isPresent()) {
                log.warn(
                        "Row {} has series number or episode number, but we're unable to parse brand from it",
                        entry
                );
                return false;
            }
            retainIfBestImage(
                    brandUri.get(),
                    bestImages,
                    baseImageExtractor.imagesFor(entry),
                    seriesNumber,
                    episodeNumber
            );
        }
        return true;
    }
    
    private void retainIfBestImage(String brandUri, Map<String, BrandImages> bestImages,
            Iterable<Image> images, Integer seriesNumber, Integer episodeNumber) {
        
        if (Iterables.isEmpty(images)) {
            return;
        }
        
        BrandImages current = bestImages.get(brandUri);
        
        if (current == null 
                || isBetterThanCurrentFavourite(current, seriesNumber, episodeNumber)) {
            bestImages.put(brandUri, 
                        new BrandImages(seriesNumber, episodeNumber, 
                                       images));
        }
    }

    private boolean isBetterThanCurrentFavourite(BrandImages current, Integer seriesNumber,
            Integer episodeNumber) {
        
        // A season image always trumps an image from an episode
        
        if (current.seriesNumber == null 
                && seriesNumber != null) {
            return true;
        }
        
        // Prefer a lower season number 
        
        if (current.seriesNumber != null 
                && seriesNumber != null
                && current.seriesNumber > seriesNumber) {
            return true;
        }
        
        // Prefer the latest episode
        
        if (current.seriesNumber == null 
                && current.episodeNumber < episodeNumber) {
            return true;
        }

        return false;
    }

    @Override
    public Void getResult() {
        return null;
    }
    
    private static class BrandImages {
        private Integer seriesNumber;
        private Integer episodeNumber;
        private Set<Image> images;
        
        public BrandImages(Integer seriesNumber, Integer episodeNumber, Iterable<Image> image) {
            this.seriesNumber = seriesNumber;
            this.episodeNumber = episodeNumber;
            this.images = ImmutableSet.copyOf(image);
        }
        
    }
    
    @Override
    public void start() {
        bestImages.clear();
    }
}

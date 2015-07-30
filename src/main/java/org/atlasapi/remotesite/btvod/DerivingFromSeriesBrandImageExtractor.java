package org.atlasapi.remotesite.btvod;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.atlasapi.media.entity.Image;
import org.atlasapi.media.entity.ImageType;
import org.atlasapi.remotesite.btvod.model.BtVodEntry;
import org.atlasapi.remotesite.btvod.model.BtVodImage;
import org.atlasapi.remotesite.btvod.model.BtVodPlproductImages;

import com.google.api.client.util.Maps;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

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

    private final String baseUrl;
    private final Map<String, BrandImage> backgroundImages = Maps.newHashMap();
    private final Map<String, BrandImage> packshotImages = Maps.newHashMap();
    private final BrandUriExtractor brandUriExtractor;
    private final BtVodSeriesUriExtractor seriesUriExtractor;

    public DerivingFromSeriesBrandImageExtractor(
            BrandUriExtractor brandUriExtractor,
            String baseUrl,
            BtVodSeriesUriExtractor seriesUriExtractor
    ) {
        this.baseUrl = checkNotNull(baseUrl);
        this.brandUriExtractor = checkNotNull(brandUriExtractor);
        this.seriesUriExtractor = checkNotNull(seriesUriExtractor);
    }
    
    @Override
    public Set<Image> imagesFor(BtVodEntry entry) {
        Optional<String> brandUri = brandUriExtractor.extractBrandUri(entry);
        if (!brandUri.isPresent()) {
            return ImmutableSet.of();
        }
        
        ImmutableSet.Builder<Image> images = ImmutableSet.builder();
        BrandImage backgroundImage = backgroundImages.get(brandUri.get());
        
        if (backgroundImage != null) {
            images.add(buildImage(backgroundImage.image, ImageType.ADDITIONAL, false));
        }
        
        BrandImage packshotImage = packshotImages.get(brandUri.get());
        if (packshotImage != null) {
            images.add(buildImage(packshotImage.image, ImageType.PRIMARY, true));
        }
        
        return images.build();
    }

    @Override
    public boolean process(BtVodEntry entry) {
        Optional<String> brandUri = brandUriExtractor.extractBrandUri(entry);
        
        if (!BrandUriExtractor.SERIES_TYPE.equals(entry.getProductType())
                && !BtVodItemWriter.EPISODE_TYPE.equals(entry.getProductType())) {
            return true;
        }
        
        Integer seriesNumber = seriesUriExtractor.extractSeriesNumber(entry).orNull();
        Integer episodeNumber = null;
        if (BtVodItemWriter.EPISODE_TYPE.equals(entry.getProductType())) {
            episodeNumber = BtVodItemWriter.extractEpisodeNumber(entry);
        }
        if (seriesNumber != null || episodeNumber != null) {
            retainIfBestImage(brandUri.get(), backgroundImages, getBackgroundImage(entry), seriesNumber, episodeNumber);
            retainIfBestImage(brandUri.get(), packshotImages, Iterables.getFirst(getPackshotImages(entry), null), seriesNumber, episodeNumber);
        }
        return true;
    }
    
    private void retainIfBestImage(String brandUri, Map<String, BrandImage> images,
            BtVodImage image, Integer seriesNumber, Integer episodeNumber) {
        
        if (image == null) {
            return;
        }
        
        BrandImage current = images.get(brandUri);
        
        if (current == null 
                || isBetterThanCurrentFavourite(current, seriesNumber, episodeNumber)) {
            images.put(brandUri, 
                        new BrandImage(seriesNumber, episodeNumber, 
                                       image));
        }
    }

    private boolean isBetterThanCurrentFavourite(BrandImage current, Integer seriesNumber,
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

    private BtVodImage getBackgroundImage(BtVodEntry row) {
        return Iterables.getFirst(row.getProductImages().getBackgroundImages(), null);
    }
    
    private List<BtVodImage> getPackshotImages(BtVodEntry entry) {
        
        BtVodPlproductImages images = entry.getProductImages();
        if (BrandUriExtractor.SERIES_TYPE.equals(entry.getProductType())) {
            if (!images.getHdPackshotDoubleImages().isEmpty()) {
                return images.getHdPackshotDoubleImages();
            } else {
                return images.getPackshotDoubleImages();
            }
        } else {
            if (!images.getHdPackshotImages().isEmpty()) {
                return images.getHdPackshotImages();
            } else {
                return images.getPackshotImages();
            }
        }
    }

    @Override
    public Void getResult() {
        return null;
    }
    
    private static class BrandImage {
        private Integer seriesNumber;
        private Integer episodeNumber;
        private BtVodImage image;
        
        public BrandImage(Integer seriesNumber, Integer episodeNumber, BtVodImage image) {
            this.seriesNumber = seriesNumber;
            this.episodeNumber = episodeNumber;
            this.image = image;
        }
        
    }
    
    private Image buildImage(BtVodImage btVodImage, ImageType imageType, boolean hasTitleArt) {
        return Image.builder(uriFor(btVodImage))
                .withHeight(btVodImage.getPlproduct$height())
                .withWidth(btVodImage.getPlproduct$width())
                .withType(imageType)
                .withHasTitleArt(hasTitleArt)
                .build();
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
        backgroundImages.clear();
        packshotImages.clear();
    }
}

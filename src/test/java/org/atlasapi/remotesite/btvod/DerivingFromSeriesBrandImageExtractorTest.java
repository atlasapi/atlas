package org.atlasapi.remotesite.btvod;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;

import org.atlasapi.media.entity.Image;
import org.atlasapi.remotesite.btvod.model.BtVodEntry;
import org.atlasapi.remotesite.btvod.model.BtVodImage;
import org.atlasapi.remotesite.btvod.model.BtVodPlproductImages;
import org.atlasapi.remotesite.btvod.model.BtVodProductMetadata;
import org.atlasapi.remotesite.btvod.model.BtVodProductScope;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;


@RunWith(MockitoJUnitRunner.class)
public class DerivingFromSeriesBrandImageExtractorTest {

    private static final String BASE_URL = "http://example.org/";
    
    private final BrandUriExtractor brandUriExtractor = mock(BrandUriExtractor.class);
    private final BtVodSeriesUriExtractor seriesUriExtractor = mock(BtVodSeriesUriExtractor.class);
    private final DerivingFromSeriesBrandImageExtractor extractor 
                    = new DerivingFromSeriesBrandImageExtractor(brandUriExtractor, seriesUriExtractor, new BtVodMpxImageExtractor(BASE_URL));
    
    @Test
    public void testExtractsImagesFromFirstSeries() {
        BtVodEntry s1 = row(1, 1, "season");
        BtVodEntry s2 = row(2, 1, "season");
        BtVodEntry s2e1 = row(2, 1, "episode");
        
        s1.getProductImages().setDoublePackshotImages(createImage("1/packshot.jpg"));
        s1.getProductImages().setDoublePackshotImagesHd(createImage("1/packshot-hd.jpg"));
        
        s2.getProductImages().setDoublePackshotImages(createImage("2/packshot.jpg"));
        s2.getProductImages().setDoublePackshotImagesHd(createImage("2/packshot-hd.jpg"));
        
        s2e1.getProductImages().setSinglePackshotImages(createImage("2/1-packshot.jpg"));
        
        when(brandUriExtractor.extractBrandUri(s1)).thenReturn(Optional.of("http://example.org/"));
        when(brandUriExtractor.extractBrandUri(s2)).thenReturn(Optional.of("http://example.org/"));
        when(brandUriExtractor.extractBrandUri(s2e1)).thenReturn(Optional.of("http://example.org/"));
        when(seriesUriExtractor.extractSeriesNumber(s1)).thenReturn(Optional.of(1));
        when(seriesUriExtractor.extractSeriesNumber(s2)).thenReturn(Optional.of(2));
        when(seriesUriExtractor.extractSeriesNumber(s2e1)).thenReturn(Optional.of(2));

        extractor.process(s1);
        extractor.process(s2);
        extractor.process(s2e1);
        
        Map<String, Image> images = Maps.uniqueIndex(extractor.imagesFor(s2), new Function<Image, String>() {

            @Override
            public String apply(Image input) {
                return input.getCanonicalUri();
            }
        });
        
        assertNotNull(images.get("http://example.org/1/packshot-hd.jpg"));
        assertNotNull(images.get("http://example.org/1/packshot.jpg"));
    }
    
    @Test
    public void testExtractsImagesFromLatestEpisode() {
        BtVodEntry s1e1 = row(1, 1, "episode");
        BtVodEntry s2e2 = row(2, 2, "episode");
          
        s1e1.getProductImages().setSinglePackshotImages(createImage("1/packshot.jpg"));
        s1e1.getProductImages().setSinglePackshotImagesHd(createImage("1/packshot-hd.jpg"));
        
        s2e2.getProductImages().setSinglePackshotImages(createImage("2/packshot.jpg"));
        s2e2.getProductImages().setSinglePackshotImagesHd(createImage("2/packshot-hd.jpg"));
        
        when(brandUriExtractor.extractBrandUri(s1e1)).thenReturn(Optional.of("http://example.org/"));
        when(brandUriExtractor.extractBrandUri(s2e2)).thenReturn(Optional.of("http://example.org/"));
        when(seriesUriExtractor.extractSeriesNumber(s1e1)).thenReturn(Optional.<Integer>absent());
        when(seriesUriExtractor.extractSeriesNumber(s2e2)).thenReturn(Optional.<Integer>absent());

        extractor.process(s1e1);
        extractor.process(s2e2);
        
        Map<String, Image> images = Maps.uniqueIndex(extractor.imagesFor(s2e2), new Function<Image, String>() {

            @Override
            public String apply(Image input) {
                return input.getCanonicalUri();
            }
        });
        
        assertNotNull(images.get("http://example.org/2/packshot-hd.jpg"));
        assertNotNull(images.get("http://example.org/2/packshot.jpg"));
    }
    
    private BtVodEntry row(int seriesNumber, int episodeNumber, String productType) {
        BtVodEntry entry = new BtVodEntry();
        entry.setGuid("abc");
        entry.setId("def");
        entry.setTitle("Test S" + seriesNumber + "-E" + episodeNumber);
        entry.setProductOfferStartDate(1364774400000L); //"Apr  1 2013 12:00AM"
        entry.setProductOfferEndDate(1398816000000L);// "Apr 30 2014 12:00AM"
        entry.setProductType(productType);
        
        BtVodProductMetadata metadata = new BtVodProductMetadata();
        metadata.setEpisodeNumber(String.valueOf(episodeNumber));
        BtVodProductScope scope = new BtVodProductScope();
        scope.setProductMetadata(metadata);
        entry.setProductScopes(ImmutableList.of(scope));
        
        entry.setProductImages(new BtVodPlproductImages());
        return entry;
    }
    
    private List<BtVodImage> createImage(String uri) {
        BtVodImage image = new BtVodImage();
        image.setPlproduct$url(uri);
        return ImmutableList.of(image);
    }
}

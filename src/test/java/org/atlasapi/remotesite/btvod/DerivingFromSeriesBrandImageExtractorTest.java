package org.atlasapi.remotesite.btvod;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
                    = new DerivingFromSeriesBrandImageExtractor(brandUriExtractor, BASE_URL, seriesUriExtractor);
    
    @Test
    public void testExtractsImagesFromLowestEpisodeNumber() {
        BtVodEntry s1e1 = row(1, 1, "1/1-background.jpg", "1/1-packshot.jpg");
        BtVodEntry s2e1 = row(2, 1, "1/2-background.jpg", "1/2-packshot.jpg");
          
        when(brandUriExtractor.extractBrandUri(s1e1)).thenReturn(Optional.of("http://example.org/"));
        when(brandUriExtractor.extractBrandUri(s2e1)).thenReturn(Optional.of("http://example.org/"));
        when(seriesUriExtractor.extractSeriesNumber(s1e1)).thenReturn(Optional.of(1));
        when(seriesUriExtractor.extractSeriesNumber(s2e1)).thenReturn(Optional.of(2));

        extractor.process(s1e1);
        extractor.process(s2e1);
        
        Map<String, Image> images = Maps.uniqueIndex(extractor.extractImages(s2e1), new Function<Image, String>() {

            @Override
            public String apply(Image input) {
                return input.getCanonicalUri();
            }
        });
        
        assertNotNull(images.get("http://example.org/1/1-background.jpg"));
        assertNotNull(images.get("http://example.org/1/1-packshot.jpg"));
    }
    
    private BtVodEntry row(int seriesNumber, int episodeNumber, String backgroundImageUri,
            String packshotImageUri) {
        BtVodEntry entry = new BtVodEntry();
        entry.setGuid("abc");
        entry.setId("def");
        entry.setTitle("Test S" + seriesNumber + "-E" + episodeNumber);
        entry.setProductOfferStartDate(1364774400000L); //"Apr  1 2013 12:00AM"
        entry.setProductOfferEndDate(1398816000000L);// "Apr 30 2014 12:00AM"
        entry.setProductType("season");
        
        BtVodProductMetadata metadata = new BtVodProductMetadata();
        metadata.setEpisodeNumber("1");
        BtVodProductScope scope = new BtVodProductScope();
        scope.setProductMetadata(metadata);
        entry.setProductScopes(ImmutableList.of(scope));
        
        BtVodImage backgroundImage = new BtVodImage();
        backgroundImage.setPlproduct$url(backgroundImageUri);
        
        BtVodImage packshotImage = new BtVodImage();
        packshotImage.setPlproduct$url(packshotImageUri);
        
        BtVodPlproductImages images = new BtVodPlproductImages();
        images.setBackgroundImages(ImmutableList.of(backgroundImage));
        images.setPackshotDoubleImages(ImmutableList.of(packshotImage));
        entry.setProductImages(images);

        return entry;
    }
}

package org.atlasapi.remotesite.btvod;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Series;
import org.atlasapi.remotesite.btvod.model.BtVodEntry;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;

@RunWith(MockitoJUnitRunner.class)
public class BtVodBrandProviderTest {

    private @Mock BrandUriExtractor brandUriExtractor;
    private @Mock BrandDescriptionUpdater brandDescriptionUpdater;
    private @Mock CertificateUpdater certificateUpdater;

    private BtVodBrandProvider brandProvider;

    private @Mock BtVodEntry seriesRow;
    private @Mock Series series;

    private @Mock BtVodEntry episodeRow;
    private @Mock Episode episode;

    private Brand brand;

    @Before
    public void setUp() throws Exception {
        brand = new Brand();
        brand.setCanonicalUri("uri");

        brandProvider = new BtVodBrandProvider(
                brandUriExtractor,
                ImmutableMap.of(brand.getCanonicalUri(), brand),
                brandDescriptionUpdater,
                certificateUpdater
        );

        when(brandUriExtractor.extractBrandUri(seriesRow))
                .thenReturn(Optional.of(brand.getCanonicalUri()));
        when(brandUriExtractor.extractBrandUri(episodeRow))
                .thenReturn(Optional.of(brand.getCanonicalUri()));
    }

    @Test
    public void testUpdateDescriptionFromSeries() throws Exception {
        brandProvider.updateBrandFromSeries(seriesRow, series);

        verify(brandDescriptionUpdater).updateDescriptions(brand, series);
    }

    @Test
    public void testUpdateCertificatesFromSeries() throws Exception {
        brandProvider.updateBrandFromSeries(seriesRow, series);

        verify(certificateUpdater).updateCertificates(brand, series);
    }

    @Test
    public void testUpdateCertificatesFromEpisode() throws Exception {
        brandProvider.updateBrandFromEpisode(episodeRow, episode);

        verify(certificateUpdater).updateCertificates(brand, episode);
    }
}
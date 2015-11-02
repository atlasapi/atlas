package org.atlasapi.remotesite.btvod;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.atlasapi.media.entity.Brand;
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

    private BtVodBrandProvider brandProvider;

    private @Mock BtVodEntry seriesRow;
    private @Mock Series series;
    private Brand brand;

    @Before
    public void setUp() throws Exception {
        brand = new Brand();
        brand.setCanonicalUri("uri");

        brandProvider = new BtVodBrandProvider(
                brandUriExtractor,
                ImmutableMap.of(brand.getCanonicalUri(), brand),
                brandDescriptionUpdater
        );
    }

    @Test
    public void testUpdateDescription() throws Exception {
        when(brandUriExtractor.extractBrandUri(seriesRow))
                .thenReturn(Optional.of(brand.getCanonicalUri()));

        brandProvider.updateDescription(seriesRow, series);

        verify(brandDescriptionUpdater).updateDescription(brand, series);
    }
}
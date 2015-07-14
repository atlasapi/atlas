package org.atlasapi.remotesite.btvod;

import com.google.common.base.Optional;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.remotesite.btvod.model.BtVodEntry;
import org.hamcrest.Matchers;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BtVodSeriesUriExtractorTest {

    private static final String BRAND_TITLE = "Brand Title";
    private static final String PRODUCT_ID = "1234";
    private static final String REAL_EPISODE_TITLE = "Real Title";
    private static final String FULL_EPISODE_TITLE = BRAND_TITLE + ": S1 S1-E9 " + REAL_EPISODE_TITLE;
    private static final Publisher PUBLISHER = Publisher.BT_VOD;


    private BrandUriExtractor brandUriExtractor = mock(BrandUriExtractor.class);
    private String brandUri = "http://brand-uri.com";


    private final BtVodSeriesUriExtractor seriesUriExtractor = new BtVodSeriesUriExtractor(
            brandUriExtractor

    );

    @Test
    public void testCanExtractSeriesUrlFromEpisode() {
        BtVodEntry row1 = new BtVodEntry();
        row1.setTitle("Cashmere Mafia S2-E2 Conference Call");

        BtVodEntry row2 = new BtVodEntry();
        row2.setTitle(FULL_EPISODE_TITLE);

        BtVodEntry row3 = new BtVodEntry();
        row3.setTitle("UFC: The Ultimate Fighter Season 19 - Season 19 Episode 2");

        BtVodEntry row4 = new BtVodEntry();
        row4.setTitle("Modern Family: S03 - HD S3-E17 Truth Be Told - HD");

        BtVodEntry row5 = new BtVodEntry();
        row5.setTitle("Being Human (USA) S2-E7 The Ties That Blind");

        String brandUri = "http://brand-uri.com";
        String brandUri2 = "http://brand-uri2.com";
        String brandUri3 = "http://brand-uri3.com";
        String brandUri4 = "http://brand-uri4.com";
        String brandUri5 = "http://brand-uri5.com";

        when(brandUriExtractor.extractBrandUri(row1)).thenReturn(Optional.of(brandUri));
        when(brandUriExtractor.extractBrandUri(row2)).thenReturn(Optional.of(brandUri2));
        when(brandUriExtractor.extractBrandUri(row3)).thenReturn(Optional.of(brandUri3));
        when(brandUriExtractor.extractBrandUri(row4)).thenReturn(Optional.of(brandUri4));
        when(brandUriExtractor.extractBrandUri(row5)).thenReturn(Optional.of(brandUri5));


        assertThat(seriesUriExtractor.seriesUriFor(row1).get(), Matchers.is(brandUri + "/series/2"));
        assertThat(seriesUriExtractor.seriesUriFor(row2).get(), Matchers.is(brandUri2 + "/series/1"));
        assertThat(seriesUriExtractor.seriesUriFor(row3).get(), Matchers.is(brandUri3 + "/series/19"));
        assertThat(seriesUriExtractor.seriesUriFor(row4).get(), Matchers.is(brandUri4 + "/series/3"));
        assertThat(seriesUriExtractor.seriesUriFor(row5).get(), Matchers.is(brandUri5 + "/series/2"));
    }

    @Test
    public void testExtractSeriesNumber() throws Exception {

    }
}
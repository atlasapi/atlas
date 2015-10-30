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

        BtVodEntry row6 = new BtVodEntry();
        row6.setTitle("Peppa Pig, Series 2, Vol. 1 - The Quarrel / The Toy Cupboard");

        BtVodEntry row7 = new BtVodEntry();
        row7.setTitle("The Thundermans S1-E1 Adventures in Supersitting");
        
        BtVodEntry row8 = new BtVodEntry();
        row8.setTitle("Mad Men S01 E08");

        String brandUri = "http://brand-uri.com";
        when(brandUriExtractor.extractBrandUri(row1)).thenReturn(Optional.of(brandUri));
        when(brandUriExtractor.extractBrandUri(row2)).thenReturn(Optional.of(brandUri));
        when(brandUriExtractor.extractBrandUri(row3)).thenReturn(Optional.of(brandUri));
        when(brandUriExtractor.extractBrandUri(row4)).thenReturn(Optional.of(brandUri));
        when(brandUriExtractor.extractBrandUri(row5)).thenReturn(Optional.of(brandUri));
        when(brandUriExtractor.extractBrandUri(row6)).thenReturn(Optional.of(brandUri));
        when(brandUriExtractor.extractBrandUri(row7)).thenReturn(Optional.of(brandUri));
        when(brandUriExtractor.extractBrandUri(row8)).thenReturn(Optional.of(brandUri));


        assertThat(seriesUriExtractor.seriesUriFor(row1).get(), Matchers.is(brandUri + "/series/2"));
        assertThat(seriesUriExtractor.seriesUriFor(row2).get(), Matchers.is(brandUri + "/series/1"));
        assertThat(seriesUriExtractor.seriesUriFor(row3).get(), Matchers.is(brandUri + "/series/19"));
        assertThat(seriesUriExtractor.seriesUriFor(row4).get(), Matchers.is(brandUri + "/series/3"));
        assertThat(seriesUriExtractor.seriesUriFor(row5).get(), Matchers.is(brandUri + "/series/2"));
        assertThat(seriesUriExtractor.seriesUriFor(row6).get(), Matchers.is(brandUri + "/series/2"));
        assertThat(seriesUriExtractor.seriesUriFor(row7).get(), Matchers.is(brandUri + "/series/1"));
        assertThat(seriesUriExtractor.seriesUriFor(row8).get(), Matchers.is(brandUri + "/series/1"));
    }

    @Test
    public void testExtractSeriesNumber() throws Exception {

    }
}
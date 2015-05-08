package org.atlasapi.remotesite.btvod;


import com.google.api.client.util.Sets;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import org.atlasapi.media.entity.ParentRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Series;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.remotesite.btvod.model.BtVodEntry;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class BtVodSeriesWriterTest {

    private static final String BRAND_TITLE = "Brand Title";
    private static final String PRODUCT_ID = "1234";
    private static final String REAL_EPISODE_TITLE = "Real Title";
    private static final String FULL_EPISODE_TITLE = BRAND_TITLE + ": S1 S1-E9 " + REAL_EPISODE_TITLE;
    private static final Publisher PUBLISHER = Publisher.BT_VOD;

    private final ContentWriter contentWriter = mock(ContentWriter.class);
    private final ContentResolver contentResolver = mock(ContentResolver.class);
    private final BtVodBrandWriter brandExtractor = mock(BtVodBrandWriter.class);
    private final BtVodDescribedFieldsExtractor extractor = mock(BtVodDescribedFieldsExtractor.class);
    private final BtVodContentListener contentListener = mock(BtVodContentListener.class);
    private final ImageUriProvider imageUriProvider = mock(ImageUriProvider.class);


    private final BtVodSeriesWriter seriesExtractor = new BtVodSeriesWriter(
            contentWriter,
            contentResolver,
            brandExtractor,
            extractor,
            PUBLISHER,
            contentListener,
            Sets.<String>newHashSet()
    );


    @Test
    public void testExtractsSeriesFromEpisode() {
        BtVodEntry entry = row();

        String brandUri = "http://brand-uri.com";
        ParentRef brandRef = mock(ParentRef.class);

        when(contentResolver.findByCanonicalUris(ImmutableSet.of(brandUri + "/series/1")))
                .thenReturn(ResolvedContent.builder().build());

        when(brandExtractor.uriFor(entry)).thenReturn(Optional.of(brandUri));
        when(brandExtractor.getBrandRefFor(entry)).thenReturn(Optional.of(brandRef));


        ArgumentCaptor<Series> captor = ArgumentCaptor.forClass(Series.class);

        seriesExtractor.process(entry);

        verify(contentWriter).createOrUpdate(captor.capture());
        Series series = captor.getValue();
        verify(extractor).setDescribedFieldsFrom(entry, series);
        assertThat(series.getCanonicalUri(), is(brandUri + "/series/1"));
        assertThat(series.getSeriesNumber(), is(1));
        assertThat(series.getParent(), is(brandRef));
        assertThat(seriesExtractor.getSeriesRefFor(entry).get().getUri(), is(brandUri + "/series/1"));
    }

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

        when(brandExtractor.uriFor(row1)).thenReturn(Optional.of(brandUri));
        when(brandExtractor.uriFor(row2)).thenReturn(Optional.of(brandUri2));
        when(brandExtractor.uriFor(row3)).thenReturn(Optional.of(brandUri3));
        when(brandExtractor.uriFor(row4)).thenReturn(Optional.of(brandUri4));
        when(brandExtractor.uriFor(row5)).thenReturn(Optional.of(brandUri5));


        assertThat(seriesExtractor.uriFor(row1).get(), Matchers.is(brandUri + "/series/2"));
        assertThat(seriesExtractor.uriFor(row2).get(), Matchers.is(brandUri2 + "/series/1"));
        assertThat(seriesExtractor.uriFor(row3).get(), Matchers.is(brandUri3 + "/series/19"));
        assertThat(seriesExtractor.uriFor(row4).get(), Matchers.is(brandUri4 + "/series/3"));
        assertThat(seriesExtractor.uriFor(row5).get(), Matchers.is(brandUri5 + "/series/2"));
    }


    private BtVodEntry row() {
        BtVodEntry entry = new BtVodEntry();
        entry.setGuid(PRODUCT_ID);
        entry.setTitle(FULL_EPISODE_TITLE);
        entry.setPlproduct$offerStartDate(1364774400000L); //"Apr  1 2013 12:00AM"
        entry.setPlproduct$offerEndDate(1398816000000L);// "Apr 30 2014 12:00AM"
        return entry;
    }
}
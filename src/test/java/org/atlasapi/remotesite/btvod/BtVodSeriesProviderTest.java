package org.atlasapi.remotesite.btvod;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import org.atlasapi.media.entity.Series;
import org.atlasapi.remotesite.btvod.model.BtVodEntry;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BtVodSeriesProviderTest {

    private static final String EXPLICIT_SERIES_GUID = "series guid";
    private static final String SUNTHESIZED_SERIES_URI = "series uri";

    private static final Series EXPLICIT_SERIES = mock(Series.class);
    private static final Series SYNTHESIZED_SERIES = mock(Series.class);

    private BtVodSeriesUriExtractor seriesUriExtractor = mock(BtVodSeriesUriExtractor.class);

    private final BtVodSeriesProvider objectUnderTest = new BtVodSeriesProvider(
            ImmutableMap.of(EXPLICIT_SERIES_GUID, EXPLICIT_SERIES),
            ImmutableMap.of(SUNTHESIZED_SERIES_URI, SYNTHESIZED_SERIES),
            seriesUriExtractor
    );



    @Test
    public void testSeriesForEpisodeInExplicitSeries() throws Exception {
        BtVodEntry row = new BtVodEntry();
        row.setParentGuid(EXPLICIT_SERIES_GUID);

        Series explicitSeries = objectUnderTest.seriesFor(row).get();

        assertThat(explicitSeries, is(EXPLICIT_SERIES));

    }

    @Test
    public void testSeriesForEpisodeSythesizedSeries() throws Exception {
        BtVodEntry row = new BtVodEntry();

        when(seriesUriExtractor.seriesUriFor(row)).thenReturn(Optional.of(SUNTHESIZED_SERIES_URI));
        Series explicitSeries = objectUnderTest.seriesFor(row).get();

        assertThat(explicitSeries, is(SYNTHESIZED_SERIES));

    }

    @Test
    public void testDontReturnSeriesIfItsNotThere() throws Exception {
        BtVodEntry row = new BtVodEntry();

        when(seriesUriExtractor.seriesUriFor(row)).thenReturn(Optional.<String>absent());

        assertThat(objectUnderTest.seriesFor(row).isPresent(), is(false));

    }
}
package org.atlasapi.remotesite.btvod;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
public class BtVodSeriesProviderTest {

    private static final String EXPLICIT_SERIES_GUID = "series guid";
    private static final String SUNTHESIZED_SERIES_URI = "series uri";

    private @Mock Series EXPLICIT_SERIES;
    private @Mock Series SYNTHESIZED_SERIES;

    private @Mock BtVodSeriesUriExtractor seriesUriExtractor;
    private @Mock CertificateUpdater certificateUpdater;

    private BtVodSeriesProvider objectUnderTest;

    @Before
    public void setUp() throws Exception {
        objectUnderTest = new BtVodSeriesProvider(
                ImmutableMap.of(EXPLICIT_SERIES_GUID, EXPLICIT_SERIES),
                ImmutableMap.of(SUNTHESIZED_SERIES_URI, SYNTHESIZED_SERIES),
                seriesUriExtractor,
                certificateUpdater
        );
    }

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

    @Test
    public void testUpdateCertificatesFromEpisode() throws Exception {
        BtVodEntry episodeRow = new BtVodEntry();
        episodeRow.setParentGuid(EXPLICIT_SERIES_GUID);
        Episode episode = new Episode();

        objectUnderTest.updateSeriesFromEpisode(episodeRow, episode);

        verify(certificateUpdater).updateCertificates(EXPLICIT_SERIES, episode);
    }
}
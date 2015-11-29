package org.atlasapi.remotesite.btvod;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.ParentRef;
import org.atlasapi.media.entity.Series;
import org.atlasapi.media.entity.TopicRef;
import org.atlasapi.remotesite.btvod.model.BtVodEntry;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

@RunWith(MockitoJUnitRunner.class)
public class BtVodSeriesProviderTest {

    private static final String EXPLICIT_SERIES_GUID = "series guid";
    private static final String SUNTHESIZED_SERIES_URI = "series uri";

    private @Mock Series EXPLICIT_SERIES;
    private @Mock Series SYNTHESIZED_SERIES;

    private @Mock ParentRef parentRef;

    private @Mock TopicRef topicRef;

    private @Mock BtVodSeriesUriExtractor seriesUriExtractor;
    private @Mock DescriptionAndImageUpdater descriptionAndImageUpdater;
    private @Mock CertificateUpdater certificateUpdater;
    private @Mock BtVodBrandProvider brandProvider;
    private @Mock TopicUpdater topicUpdater;
    private @Mock BtVodContentListener listener;

    private BtVodSeriesProvider seriesProvider;
    private int seriesNumber;

    @Before
    public void setUp() throws Exception {
        when(EXPLICIT_SERIES.getParent()).thenReturn(parentRef);
        seriesNumber = 4;
        when(EXPLICIT_SERIES.getSeriesNumber()).thenReturn(seriesNumber);

        seriesProvider = new BtVodSeriesProvider(
                ImmutableMap.of(EXPLICIT_SERIES_GUID, EXPLICIT_SERIES),
                ImmutableMap.of(SUNTHESIZED_SERIES_URI, SYNTHESIZED_SERIES),
                seriesUriExtractor,
                descriptionAndImageUpdater,
                certificateUpdater,
                brandProvider,
                topicUpdater,
                listener
        );
    }

    @Test
    public void testSeriesForEpisodeInExplicitSeries() throws Exception {
        BtVodEntry row = new BtVodEntry();
        row.setParentGuid(EXPLICIT_SERIES_GUID);

        Series explicitSeries = seriesProvider.seriesFor(row).get();

        assertThat(explicitSeries, is(EXPLICIT_SERIES));
    }

    @Test
    public void testSeriesForEpisodeSythesizedSeries() throws Exception {
        BtVodEntry row = new BtVodEntry();

        when(seriesUriExtractor.seriesUriFor(row)).thenReturn(Optional.of(SUNTHESIZED_SERIES_URI));
        Series explicitSeries = seriesProvider.seriesFor(row).get();

        assertThat(explicitSeries, is(SYNTHESIZED_SERIES));
    }

    @Test
    public void testSeriesForEpisodeInExplicitSeriesButWithoutParentGuid() throws Exception {
        BtVodEntry row = new BtVodEntry();

        when(seriesUriExtractor.seriesUriFor(row)).thenReturn(Optional.of("some other uri"));
        when(brandProvider.brandRefFor(row)).thenReturn(Optional.of(parentRef));
        when(seriesUriExtractor.extractSeriesNumber(row)).thenReturn(Optional.of(seriesNumber));

        Series series = seriesProvider.seriesFor(row).get();

        assertThat(series, is(EXPLICIT_SERIES));
    }

    @Test
    public void testDoNotReturnSeriesIfItsNotThere() throws Exception {
        BtVodEntry row = new BtVodEntry();

        when(seriesUriExtractor.seriesUriFor(row)).thenReturn(Optional.<String>absent());
        when(brandProvider.brandRefFor(row)).thenReturn(Optional.<ParentRef>absent());

        assertThat(seriesProvider.seriesFor(row).isPresent(), is(false));
    }

    @Test
    public void testUpdateDescriptionsAndImagesFromEpisode() throws Exception {
        BtVodEntry episodeRow = new BtVodEntry();
        episodeRow.setParentGuid(EXPLICIT_SERIES_GUID);
        Episode episode = new Episode();

        seriesProvider.updateSeriesFromEpisode(episodeRow, episode);

        verify(descriptionAndImageUpdater).update(EXPLICIT_SERIES, episode);
    }

    @Test
    public void testUpdateCertificatesFromEpisode() throws Exception {
        BtVodEntry episodeRow = new BtVodEntry();
        episodeRow.setParentGuid(EXPLICIT_SERIES_GUID);
        Episode episode = new Episode();

        seriesProvider.updateSeriesFromEpisode(episodeRow, episode);

        verify(certificateUpdater).updateCertificates(EXPLICIT_SERIES, episode);
    }

    @Test
    public void testUpdateTopicsFromEpisode() throws Exception {
        BtVodEntry episodeRow = new BtVodEntry();
        episodeRow.setParentGuid(EXPLICIT_SERIES_GUID);
        Episode episode = new Episode();
        episode.addTopicRef(topicRef);

        seriesProvider.updateSeriesFromEpisode(episodeRow, episode);

        verify(topicUpdater).updateTopics(EXPLICIT_SERIES, ImmutableList.of(topicRef));
    }

    @Test
    public void testCallListenerAfterUpdatingFromEpisode() throws Exception {
        BtVodEntry episodeRow = new BtVodEntry();
        episodeRow.setParentGuid(EXPLICIT_SERIES_GUID);
        Episode episode = new Episode();

        seriesProvider.updateSeriesFromEpisode(episodeRow, episode);

        verify(listener).onContent(EXPLICIT_SERIES, episodeRow);
    }

    @Test
    public void testCanConstructProviderFromSeriesWithNullParentAndNullSeriesNumber()
            throws Exception {
        Series series = mock(Series.class);

        when(series.getParent()).thenReturn(null);
        when(series.getSeriesNumber()).thenReturn(null);

        new BtVodSeriesProvider(
                ImmutableMap.of("guid", series),
                ImmutableMap.<String, Series>of(),
                seriesUriExtractor,
                descriptionAndImageUpdater,
                certificateUpdater,
                brandProvider,
                topicUpdater,
                listener
        );
    }

    @Test
    public void testCanConstructProviderWithDuplicateSeries() throws Exception {
        Series series = mock(Series.class);

        when(series.getParent()).thenReturn(mock(ParentRef.class));
        when(series.getSeriesNumber()).thenReturn(5);

        new BtVodSeriesProvider(
                ImmutableMap.of("guid1", series, "guid2", series),
                ImmutableMap.<String, Series>of(),
                seriesUriExtractor,
                descriptionAndImageUpdater,
                certificateUpdater,
                brandProvider,
                topicUpdater,
                listener
        );
    }
}
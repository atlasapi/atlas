package org.atlasapi.remotesite.btvod;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Image;
import org.atlasapi.media.entity.Series;
import org.atlasapi.media.entity.TopicRef;
import org.atlasapi.remotesite.btvod.model.BtVodEntry;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class BtVodBrandProviderTest {

    private @Mock BrandUriExtractor brandUriExtractor;
    private @Mock HierarchyDescriptionAndImageUpdater descriptionAndImageUpdater;
    private @Mock CertificateUpdater certificateUpdater;
    private @Mock TopicUpdater topicUpdater;
    private @Mock BtVodContentListener listener;

    private BtVodBrandProvider brandProvider;

    private @Mock BtVodEntry seriesRow;
    private @Mock Series series;

    private @Mock BtVodEntry episodeRow;
    private @Mock Episode episode;

    private @Mock TopicRef topicRef;

    private Brand brand;
    private String guid;

    @Before
    public void setUp() throws Exception {
        brand = new Brand();
        brand.setCanonicalUri("uri");
        guid = "guid";

        brandProvider = new BtVodBrandProvider(
                brandUriExtractor,
                ImmutableMap.of(brand.getCanonicalUri(), brand),
                ImmutableMap.of(guid, brand),
                descriptionAndImageUpdater,
                certificateUpdater,
                topicUpdater,
                listener
        );

        when(brandUriExtractor.extractBrandUri(seriesRow))
                .thenReturn(Optional.of(brand.getCanonicalUri()));
        when(brandUriExtractor.extractBrandUri(episodeRow))
                .thenReturn(Optional.of(brand.getCanonicalUri()));
    }

    @Test
    public void testUpdateDescriptionsAndImagesFromSeries() throws Exception {
        brandProvider.updateBrandFromSeries(seriesRow, series);

        verify(descriptionAndImageUpdater).update(brand, series, seriesRow);
    }

    @Test
    public void testUpdateDescriptionsAndImagesFromEpisode() throws Exception {
        brandProvider.updateBrandFromEpisode(episodeRow, episode);

        verify(descriptionAndImageUpdater).update(brand, episode, episodeRow);
    }

    @Test
    public void testUpdateDescriptionsAndImagesFromCollection() throws Exception {
        BtVodCollection collection = new BtVodCollection(guid,
                DateTime.now(), "desc", "descL", ImmutableSet.<Image>of()
        );
        brandProvider.updateBrandFromCollection(
                collection
        );

        verify(descriptionAndImageUpdater).update(brand, collection);
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

    @Test
    public void testUpdateTopicsFromSeries() throws Exception {
        when(series.getTopicRefs()).thenReturn(ImmutableList.of(topicRef));

        brandProvider.updateBrandFromSeries(seriesRow, series);

        verify(topicUpdater).updateTopics(brand, ImmutableList.of(topicRef));
    }

    @Test
    public void testUpdateTopicsFromEpisode() throws Exception {
        when(episode.getTopicRefs()).thenReturn(ImmutableList.of(topicRef));

        brandProvider.updateBrandFromEpisode(episodeRow, episode);

        verify(topicUpdater).updateTopics(brand, ImmutableList.of(topicRef));
    }

    @Test
    public void testCallListenerAfterUpdatingFromSeries() throws Exception {
        brandProvider.updateBrandFromSeries(seriesRow, series);

        verify(listener).onContent(brand, seriesRow);
    }

    @Test
    public void testCallListenerAfterUpdatingFromEpisode() throws Exception {
        brandProvider.updateBrandFromEpisode(episodeRow, episode);

        verify(listener).onContent(brand, episodeRow);
    }
}
